package io.github.swan28pande.ingest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/** YAML configuration (see config.example.yaml). */
public record Config(
        Ports ports,
        Kafka kafka,
        Dedup dedup,
        List<Source> sources) {

    public record Ports(int http, int grpc, int metrics) {}

    /** {@code properties} are passed straight to the Kafka producer. */
    public record Kafka(String bootstrapServers, Map<String, String> properties) {}

    public record Dedup(long ttlSeconds, int maxEntries) {}

    public enum Transport { http, grpc }

    public record Source(
            String name,
            String topic,
            String dlqTopic,              // optional: where invalid records go
            String keyField,              // optional: Kafka message key
            List<String> dedupFields,     // optional: identity fields; default = whole record
            List<Transport> transports,   // optional: default = both
            boolean allowUnknownFields,
            Map<String, Field> schema) {

        public boolean accepts(Transport t) {
            return transports == null || transports.isEmpty() || transports.contains(t);
        }
    }

    public enum Type { string, integer, number, bool, object, array }

    public record Field(Type type, boolean required, List<String> allowed) {}

    public static Config load(Path path) throws IOException {
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        Config c = yaml.readValue(path.toFile(), Config.class);
        for (Source s : c.sources()) {
            if (s.name() == null || s.topic() == null || s.schema() == null) {
                throw new IllegalArgumentException("every source needs name, topic and schema");
            }
        }
        return c;
    }
}
