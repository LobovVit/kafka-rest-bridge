# build
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /kafka-rest-bridge main.go

# run
FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=build /kafka-rest-bridge /kafka-rest-bridge
USER nonroot:nonroot
ENTRYPOINT ["/kafka-rest-bridge"]