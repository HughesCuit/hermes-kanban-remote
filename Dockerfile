# Multi-stage build
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /hermes-cluster ./cmd/cluster

FROM alpine:3.19
RUN apk --no-cache add ca-certificates curl
WORKDIR /app
COPY --from=builder /hermes-cluster /usr/local/bin/hermes-cluster
COPY --from=builder /app/cluster.yaml /app/cluster.yaml
EXPOSE 8787
ENTRYPOINT ["hermes-cluster"]
CMD ["-config", "cluster.yaml"]
