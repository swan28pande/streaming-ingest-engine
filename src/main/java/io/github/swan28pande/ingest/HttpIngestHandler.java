package io.github.swan28pande.ingest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * {@code POST /v1/sources/{source}/records}
 * Body: a JSON object, a JSON array of objects, or NDJSON ({@code Content-Type: application/x-ndjson}).
 */
final class HttpIngestHandler implements HttpHandler {
    private static final String PREFIX = "/v1/sources/";
    private static final int MAX_BODY = 16 * 1024 * 1024;

    private final IngestPipeline pipeline;

    HttpIngestHandler(IngestPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try (ex) {
            String path = ex.getRequestURI().getPath();
            if (!path.startsWith(PREFIX) || !path.endsWith("/records")) {
                reply(ex, 404, error("not found"));
                return;
            }
            if (!"POST".equals(ex.getRequestMethod())) {
                reply(ex, 405, error("use POST"));
                return;
            }
            String name = path.substring(PREFIX.length(), path.length() - "/records".length());

            Config.Source src;
            List<JsonNode> records;
            try {
                src = pipeline.source(name, Config.Transport.http);
                records = parse(ex);
            } catch (IngestPipeline.UnknownSourceException e) {
                reply(ex, 404, error(e.getMessage()));
                return;
            } catch (IllegalArgumentException | JsonProcessingException e) {
                reply(ex, 400, error("bad body: " + e.getMessage()));
                return;
            }

            try {
                IngestPipeline.Result r = pipeline.ingest(src, Config.Transport.http, records, 0);
                ObjectNode body = IngestPipeline.JSON.createObjectNode()
                        .put("accepted", r.accepted()).put("duplicates", r.duplicates()).put("invalid", r.invalid());
                body.set("errors", IngestPipeline.JSON.valueToTree(r.errors()));
                reply(ex, 200, body);
            } catch (IngestPipeline.PublishException e) {
                reply(ex, 503, error(e.getMessage())); // retryable
            }
        }
    }

    private static List<JsonNode> parse(HttpExchange ex) throws IOException {
        byte[] body = ex.getRequestBody().readNBytes(MAX_BODY + 1);
        if (body.length > MAX_BODY) throw new IllegalArgumentException("body exceeds " + MAX_BODY + " bytes");
        String ct = String.valueOf(ex.getRequestHeaders().getFirst("Content-Type"));
        List<JsonNode> out = new ArrayList<>();
        if (ct.startsWith("application/x-ndjson")) {
            var reader = new BufferedReader(new InputStreamReader(
                    new java.io.ByteArrayInputStream(body), StandardCharsets.UTF_8));
            for (String line; (line = reader.readLine()) != null; ) {
                if (!line.isBlank()) out.add(IngestPipeline.JSON.readTree(line));
            }
            return out;
        }
        JsonNode root = IngestPipeline.JSON.readTree(body);
        if (root == null) throw new IllegalArgumentException("empty body");
        if (root.isArray()) root.forEach(out::add);
        else out.add(root);
        return out;
    }

    private static ObjectNode error(String msg) {
        return IngestPipeline.JSON.createObjectNode().put("error", msg);
    }

    static void reply(HttpExchange ex, int status, JsonNode body) throws IOException {
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
