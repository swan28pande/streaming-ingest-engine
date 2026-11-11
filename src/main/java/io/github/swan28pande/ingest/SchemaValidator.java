package io.github.swan28pande.ingest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/** Checks a JSON record against a source's field schema. Returns human-readable violations. */
final class SchemaValidator {
    private SchemaValidator() {}

    static List<String> validate(JsonNode record, Config.Source source) {
        List<String> errors = new ArrayList<>();
        if (record == null || !record.isObject()) {
            errors.add("record must be a JSON object");
            return errors;
        }
        for (Map.Entry<String, Config.Field> e : source.schema().entrySet()) {
            String name = e.getKey();
            Config.Field f = e.getValue();
            JsonNode v = record.get(name);
            if (v == null || v.isNull()) {
                if (f.required()) errors.add(name + ": required");
                continue;
            }
            if (!matches(v, f.type())) {
                errors.add(name + ": expected " + f.type() + ", got " + v.getNodeType().name().toLowerCase());
            } else if (f.allowed() != null && !f.allowed().isEmpty() && !f.allowed().contains(v.asText())) {
                errors.add(name + ": must be one of " + f.allowed());
            }
        }
        if (!source.allowUnknownFields()) {
            for (Iterator<String> it = record.fieldNames(); it.hasNext(); ) {
                String name = it.next();
                if (!source.schema().containsKey(name)) errors.add(name + ": unknown field");
            }
        }
        return errors;
    }

    private static boolean matches(JsonNode v, Config.Type type) {
        return switch (type) {
            case string -> v.isTextual();
            case integer -> v.isIntegralNumber() || (v.isNumber() && v.asDouble() == Math.rint(v.asDouble()));
            case number -> v.isNumber();
            case bool -> v.isBoolean();
            case object -> v.isObject();
            case array -> v.isArray();
        };
    }
}
