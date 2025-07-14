package com.aerospike.load;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for parsing JSON with relaxed syntax using Jackson.
 * Supports unquoted field names, single quotes, and automatic type coercion.
 */
public class RelaxedJsonMapper {

    private static final ObjectMapper RELAXED_MAPPER = new ObjectMapper();
    private static final ObjectMapper STANDARD_MAPPER = new ObjectMapper();

    static {
        // Configure relaxed mapper to allow JSON supersets
        RELAXED_MAPPER.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
        RELAXED_MAPPER.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
        RELAXED_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        // Use regular integers instead of BigInteger for better compatibility
        RELAXED_MAPPER.disable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);
    }

    /**
     * Parse JSON from a FileReader.
     * @param reader FileReader containing JSON data
     * @return JsonNode representing the parsed JSON
     * @throws IOException if parsing fails
     */
    public static JsonNode parseJson(FileReader reader) throws IOException {
        return RELAXED_MAPPER.readTree(reader);
    }

    /**
     * Parse JSON from a string.
     * @param json JSON string
     * @return JsonNode representing the parsed JSON
     * @throws IOException if parsing fails
     */
    public static JsonNode parseJson(String json) throws IOException {
        return RELAXED_MAPPER.readTree(json);
    }

    /**
     * Parse JSON string into a Map<String, Object>.
     * @param json JSON string
     * @return Map representation of the JSON
     * @throws IOException if parsing fails
     */
    public static Map<String, Object> parseJsonToMap(String json) throws IOException {
        return RELAXED_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
    }

    /**
     * Parse JSON string into a List<Object>.
     * @param json JSON string
     * @return List representation of the JSON
     * @throws IOException if parsing fails
     */
    public static List<Object> parseJsonToList(String json) throws IOException {
        return RELAXED_MAPPER.readValue(json, new TypeReference<List<Object>>() {});
    }

    /**
     * Parse JSON with automatic key type coercion (similar to the sample code).
     * This method converts string keys to appropriate types (Integer, Long, Double, Boolean).
     * @param json JSON string
     * @return Map<Object, Object> with coerced keys
     * @throws IOException if parsing fails
     */
    public static Map<Object, Object> parseJsonWithKeyCoercion(String json) throws IOException {
        // First pass: everything is still strings for keys
        Map<String, Object> intermediate = RELAXED_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});

        // Second pass: coerce keys to proper types
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : intermediate.entrySet()) {
            result.put(coerceKey(entry.getKey()), entry.getValue());
        }
        return result;
    }

    /**
     * Tries to turn a textual key into Integer, Long, Double, or Boolean.
     * Falls back to the original String if none match.
     * @param key The string key to coerce
     * @return The coerced key or original string if no coercion is possible
     */
    private static Object coerceKey(String key) {
        try { 
            return Integer.valueOf(key); 
        } catch (NumberFormatException ignored) {}
        
        try { 
            return Long.valueOf(key); 
        } catch (NumberFormatException ignored) {}
        
        try { 
            return Double.valueOf(key); 
        } catch (NumberFormatException ignored) {}
        
        try { 
            return Float.valueOf(key); 
        } catch (NumberFormatException ignored) {}
        
        if ("true".equalsIgnoreCase(key) || "false".equalsIgnoreCase(key)) {
            return Boolean.valueOf(key);
        }
        
        return key; // leave it a String
    }

    /**
     * Convert a JsonNode to a Java object (Map, List, or primitive).
     * @param node The JsonNode to convert
     * @return The converted Java object
     */
    public static Object jsonNodeToObject(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else if (node.isInt()) {
            return node.asInt();
        } else if (node.isLong()) {
            return node.asLong();
        } else if (node.isFloat()) {
            return node.floatValue();
        } else if (node.isDouble()) {
            return node.asDouble();
        } else if (node.isTextual()) {
            return node.asText();
        } else if (node.isArray()) {
            return STANDARD_MAPPER.convertValue(node, List.class);
        } else if (node.isObject()) {
            return STANDARD_MAPPER.convertValue(node, Map.class);
        }
        return node.toString();
    }

    /**
     * Get a field value from a JsonNode.
     * @param node The JsonNode to get the field from
     * @param fieldName The name of the field
     * @return The field value as Object, or null if not found
     */
    public static Object getFromJsonNode(JsonNode node, String fieldName) {
        if (node == null || !node.has(fieldName)) {
            return null;
        }
        return jsonNodeToObject(node.get(fieldName));
    }

    /**
     * Check if a JsonNode has a specific field.
     * @param node The JsonNode to check
     * @param fieldName The name of the field
     * @return true if the field exists, false otherwise
     */
    public static boolean hasField(JsonNode node, String fieldName) {
        return node != null && node.has(fieldName);
    }

    /**
     * Get a JsonNode as a string representation.
     * @param node The JsonNode to convert
     * @return String representation of the JsonNode
     */
    public static String jsonNodeToString(JsonNode node) {
        return node != null ? node.toString() : null;
    }
} 