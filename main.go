package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers      []string
	InputTopic   string
	OutputTopic  string
	GroupID      string
	RestURL      string
	HTTPMethod   string
	HTTPHeaders  map[string]string
	Timeout      time.Duration
	MaxRetries   int
	BackoffStart time.Duration
	BackoffMax   time.Duration
	DLQTopic     string // optional
	InsecureTLS  bool   // optional: allow self-signed
}

func mustEnv(key string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		log.Fatalf("missing required env: %s", key)
	}
	return v
}

func getEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func parseHeaders(s string) map[string]string {
	// format: "K1:V1;K2:V2"
	res := map[string]string{}
	s = strings.TrimSpace(s)
	if s == "" {
		return res
	}
	pairs := strings.Split(s, ";")
	for _, p := range pairs {
		if strings.TrimSpace(p) == "" {
			continue
		}
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			log.Printf("WARN: skip bad header pair: %q", p)
			continue
		}
		res[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return res
}

func loadConfig() Config {
	timeoutMs, _ := strconv.Atoi(getEnv("HTTP_TIMEOUT_MS", "10000"))
	maxRetries, _ := strconv.Atoi(getEnv("MAX_RETRIES", "5"))
	backoffStartMs, _ := strconv.Atoi(getEnv("BACKOFF_START_MS", "200"))
	backoffMaxMs, _ := strconv.Atoi(getEnv("BACKOFF_MAX_MS", "5000"))
	insecure := strings.EqualFold(getEnv("INSECURE_TLS", "false"), "true")

	return Config{
		Brokers:      strings.Split(mustEnv("KAFKA_BROKERS"), ","),
		InputTopic:   mustEnv("INPUT_TOPIC"),
		OutputTopic:  mustEnv("OUTPUT_TOPIC"),
		GroupID:      mustEnv("GROUP_ID"),
		RestURL:      mustEnv("REST_URL"),
		HTTPMethod:   strings.ToUpper(getEnv("HTTP_METHOD", "POST")),
		HTTPHeaders:  parseHeaders(getEnv("HTTP_HEADERS", "")),
		Timeout:      time.Duration(timeoutMs) * time.Millisecond,
		MaxRetries:   maxRetries,
		BackoffStart: time.Duration(backoffStartMs) * time.Millisecond,
		BackoffMax:   time.Duration(backoffMaxMs) * time.Millisecond,
		DLQTopic:     getEnv("DLQ_TOPIC", ""),
		InsecureTLS:  insecure,
	}
}

func newHTTPClient(cfg Config) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.InsecureTLS}, //nolint:gosec
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	return &http.Client{
		Timeout:   cfg.Timeout,
		Transport: transport,
	}
}

func exponentialBackoff(attempt int, start, max time.Duration) time.Duration {
	d := start * (1 << attempt)
	if d > max {
		return max
	}
	return d
}

func callREST(ctx context.Context, client *http.Client, cfg Config, body []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, cfg.HTTPMethod, cfg.RestURL, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	// default content type if not provided
	if _, ok := cfg.HTTPHeaders["Content-Type"]; !ok {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range cfg.HTTPHeaders {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return buf.Bytes(), resp.StatusCode, nil
	}
	return buf.Bytes(), resp.StatusCode, fmt.Errorf("non-2xx status: %d", resp.StatusCode)
}

func main() {
	cfg := loadConfig()
	log.Printf("starting kafka-rest-bridge | in=%s out=%s dlq=%s method=%s url=%s",
		cfg.InputTopic, cfg.OutputTopic, cfg.DLQTopic, cfg.HTTPMethod, cfg.RestURL)

	// graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Kafka consumer (Reader)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.InputTopic,
		MinBytes:       1,        // 1B
		MaxBytes:       10 << 20, // 10MB
		CommitInterval: 0,        // manual commit
	})
	defer reader.Close()

	// Kafka producer (Writer) for output
	writerOut := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.OutputTopic,
		Balancer:     &kafka.Hash{}, // keep key affinity
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	defer writerOut.Close()

	// Optional DLQ
	var writerDLQ *kafka.Writer
	if cfg.DLQTopic != "" {
		writerDLQ = &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.DLQTopic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		}
		defer writerDLQ.Close()
	}

	httpClient := newHTTPClient(cfg)

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("context canceled, shutting down")
				return
			}
			log.Printf("ERROR: fetch message: %v", err)
			continue
		}

		// prepare headers passthrough (Kafka headers -> HTTP? We keep them for output write)
		// For HTTP call we send only body; headers are configured via env

		var respBody []byte
		var status int
		var lastErr error

		// retry with backoff
		for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
			callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			respBody, status, lastErr = callREST(callCtx, httpClient, cfg, m.Value)
			cancel()

			if lastErr == nil {
				break
			}
			sleep := exponentialBackoff(attempt, cfg.BackoffStart, cfg.BackoffMax)
			log.Printf("WARN: REST call failed (attempt %d/%d): %v (status=%d). retry in %s",
				attempt+1, cfg.MaxRetries+1, lastErr, status, sleep)
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				lastErr = ctx.Err()
				break
			}
		}

		if lastErr != nil {
			// push to DLQ if configured, commit to move on; else do NOT commit (will retry on restart)
			if writerDLQ != nil {
				dlqMsg := kafka.Message{
					Key:   m.Key,
					Value: m.Value, // original payload
					Headers: append(m.Headers, []kafka.Header{
						{Key: "rest_error", Value: []byte(lastErr.Error())},
						{Key: "rest_status", Value: []byte(strconv.Itoa(status))},
						{Key: "rest_url", Value: []byte(cfg.RestURL)},
					}...),
					Time: time.Now(),
				}
				if err := writerDLQ.WriteMessages(ctx, dlqMsg); err != nil {
					log.Printf("ERROR: write to DLQ failed: %v (message will be retried later, not committing)", err)
					// don't commit; message will be reprocessed
					continue
				}
				// commit since we've safely diverted to DLQ
				if err := reader.CommitMessages(ctx, m); err != nil {
					log.Printf("ERROR: commit after DLQ: %v", err)
				}
				continue
			}

			// No DLQ: don't commit so we can retry later.
			log.Printf("ERROR: REST permanently failed and no DLQ configured. Not committing offset. err=%v", lastErr)
			// small pause to avoid hot loop
			time.Sleep(2 * time.Second)
			continue
		}

		// success: write response to output topic
		outMsg := kafka.Message{
			Key:   m.Key,
			Value: respBody,
			// propagate headers, plus HTTP status
			Headers: append(m.Headers, kafka.Header{Key: "http_status", Value: []byte(strconv.Itoa(status))}),
			Time:    time.Now(),
		}
		if err := writerOut.WriteMessages(ctx, outMsg); err != nil {
			log.Printf("ERROR: write to output failed: %v (not committing; will retry message)", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// commit input after output successful
		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("ERROR: commit failed: %v", err)
			// it's okay; on re-delivery we'll be idempotent at sink if possible
		}
	}
}
