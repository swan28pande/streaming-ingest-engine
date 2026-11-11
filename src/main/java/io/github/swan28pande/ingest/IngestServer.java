package io.github.swan28pande.ingest;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpServer;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;

/** Entry point: {@code java -jar ingest-engine.jar [config.yaml]}. */
public final class IngestServer {
    private static final Logger log = Logger.getLogger(IngestServer.class.getName());

    public static void main(String[] args) throws Exception {
        Config cfg = Config.load(Path.of(args.length > 0 ? args[0] : "config.yaml"));
        Deduplicator dedup = new Deduplicator(cfg.dedup().ttlSeconds(), cfg.dedup().maxEntries());
        Metrics metrics = new Metrics(dedup);
        IngestPipeline pipeline = new IngestPipeline(cfg, dedup, metrics);

        HttpServer http = HttpServer.create(new InetSocketAddress(cfg.ports().http()), 0);
        http.createContext("/v1/sources/", new HttpIngestHandler(pipeline));
        http.createContext("/healthz", ex -> HttpIngestHandler.reply(ex, 200,
                IngestPipeline.JSON.createObjectNode().put("status", "ok")));
        http.setExecutor(Executors.newFixedThreadPool(32));
        http.start();

        Server grpc = ServerBuilder.forPort(cfg.ports().grpc())
                .addService(new GrpcIngestService(pipeline))
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();

        metrics.serve(cfg.ports().metrics());
        log.info(String.format("ingest engine up: http :%d, grpc :%d, metrics :%d/metrics, sources %s",
                cfg.ports().http(), cfg.ports().grpc(), cfg.ports().metrics(),
                cfg.sources().stream().map(Config.Source::name).toList()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            http.stop(5);
            grpc.shutdown();
            try {
                grpc.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            pipeline.close(); // flushes in-flight Kafka sends
            metrics.close();
        }));
        grpc.awaitTermination();
    }
}
