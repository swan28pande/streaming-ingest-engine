# Streaming Data Ingestion Engine

A Java service that accepts records from configured sources over **HTTP** and **gRPC**, validates them against each source's schema, drops duplicates, and publishes them to **Kafka**. It exposes **Prometheus** metrics throughout.

**Stack:** Java 17 · Kafka · gRPC · Prometheus

## Pipeline

```
HTTP  POST /v1/sources/{source}/records ─┐
gRPC  Ingest / IngestStream ─────────────┴─► validate ──► dedup ──► Kafka topic
                                               │ invalid
                                               └─────────────────► DLQ topic (optional)
```

1. **Sources** are declared in YAML. Each one has a topic, an optional dead-letter topic, the transports it accepts, a Kafka key field and a schema.
2. **Validation** checks field types (`string`, `integer`, `number`, `bool`, `object`, `array`), required fields and allowed values, and rejects unknown fields unless `allowUnknownFields` is set. Every violation is reported back with the record's index.
3. **Deduplication** builds a key from the configured `dedupFields`, or from a SHA-256 of the whole record with key order ignored. The key is held in a bounded cache with a TTL. It's reserved before publishing and released if Kafka fails, so retrying the request is safe.
4. **Publishing** uses an idempotent producer (`acks=all`). A request returns only after Kafka has acknowledged every accepted record. If Kafka fails, the client gets HTTP 503 or gRPC `UNAVAILABLE` and can retry.

## Run

```bash
mvn package
cp config.example.yaml config.yaml          # edit sources, Kafka address
java -jar target/ingest-engine.jar config.yaml

# or: Kafka + engine + Prometheus (UI on :9091)
docker compose up --build
```

## Send data

```bash
# HTTP: a JSON object, a JSON array, or NDJSON
curl -s localhost:8080/v1/sources/orders/records -H 'Content-Type: application/json' -d '[
  {"order_id":"o-1","customer_id":"c-7","amount":42.5,"currency":"USD","status":"paid"},
  {"order_id":"o-1","customer_id":"c-7","amount":42.5,"currency":"USD","status":"paid"},
  {"order_id":"o-2","amount":"ten","currency":"JPY","status":"paid"}
]'
# {"accepted":1,"duplicates":1,"invalid":1,"errors":[{"index":2,"message":"customer_id: required; amount: expected number, got string; currency: must be one of [USD, EUR, GBP, INR]"}]}

# gRPC (server reflection is on)
grpcurl -plaintext -d '{"source":"clickstream","records":["{\"user_id\":\"u1\",\"url\":\"/\",\"ts_ms\":1}"]}' \
  localhost:9090 ingest.v1.IngestService/Ingest
```

`IngestStream` is client-streaming. Send many batches over one stream and get a single summary at the end. The service is defined in `src/main/proto/ingest.proto`.

## Metrics (`:9100/metrics`)

| Metric | |
|---|---|
| `ingest_records_total{source,transport,outcome}` | outcome = `accepted`, `duplicate`, `invalid` or `failed` |
| `ingest_publish_latency_seconds{source}` | batch received → all Kafka acks |
| `ingest_batch_size_records{source}` | records per request |
| `ingest_dedup_cache_entries` | keys held by the dedup cache |
| `jvm_*` | JVM memory, GC and threads |

## Limitations

This is a starting skeleton:
- **Dedup is per process.** Behind a load balancer, either route each key to one instance or back the `Deduplicator` with a shared store such as Redis.
- **No authentication or TLS** on HTTP or gRPC.
- **Schemas are a small built-in format.** Schema Registry, Avro and Protobuf payloads are natural next steps.
- **Sources are push-only.** Pull connectors, such as polling an API or tailing a database, would sit on the same `IngestPipeline`.
