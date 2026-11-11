package io.github.swan28pande.ingest;

import java.io.IOException;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;

/** Prometheus metrics, served at :{metrics port}/metrics. */
final class Metrics {
    final Counter records = Counter.builder()
            .name("ingest_records_total")
            .help("Records received, by source, transport and outcome")
            .labelNames("source", "transport", "outcome") // accepted | duplicate | invalid | failed
            .register();

    final Histogram publishLatency = Histogram.builder()
            .name("ingest_publish_latency_seconds")
            .help("Time from receiving a batch to Kafka acknowledging all of it")
            .labelNames("source")
            .register();

    final Histogram batchSize = Histogram.builder()
            .name("ingest_batch_size_records")
            .help("Records per ingest request")
            .classicUpperBounds(1, 10, 100, 1000, 10000)
            .labelNames("source")
            .register();

    private HTTPServer server;

    Metrics(Deduplicator dedup) {
        GaugeWithCallback.builder()
                .name("ingest_dedup_cache_entries")
                .help("Keys currently held by the deduplication cache")
                .callback(cb -> cb.call(dedup.size()))
                .register();
        JvmMetrics.builder().register();
    }

    void serve(int port) throws IOException {
        server = HTTPServer.builder().port(port).buildAndStart();
    }

    void close() {
        if (server != null) server.close();
    }
}
