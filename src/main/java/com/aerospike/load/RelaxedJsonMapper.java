package com.aerospike.load;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger log = LogManager.getLogger(RelaxedJsonMapper.class);

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
        // First parse as a regular map with string keys
        Map<String, Object> stringKeyMap = RELAXED_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
        
        // Create a new map with coerced keys
        Map<Object, Object> coercedMap = new LinkedHashMap<>();
        
        for (Map.Entry<String, Object> entry : stringKeyMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            Object coercedKey = coerceKey(key);
            coercedMap.put(coercedKey, value);
        }
        
        return coercedMap;
    }

    /**
     * Coerce a string key to its appropriate type (Integer, Long, Double, Boolean, or String).
     * @param key The string key to coerce
     * @return The coerced key object
     */
    private static Object coerceKey(String key) {
        if (key == null) {
            return key;
        }
        
        // Try to parse as boolean
        if ("true".equalsIgnoreCase(key)) {
            return true;
        }
        if ("false".equalsIgnoreCase(key)) {
            return false;
        }
        
        // Try to parse as integer
        try {
            if (!key.contains(".") && !key.contains("e") && !key.contains("E")) {
                long longValue = Long.parseLong(key);
                if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                    return (int) longValue;
                } else {
                    return longValue;
                }
            }
        } catch (NumberFormatException e) {
            // Not an integer
        }
        
        // Try to parse as double
        try {
            return Double.parseDouble(key);
        } catch (NumberFormatException e) {
            // Not a double
        }
        
        // Return as string
        return key;
    }

    /**
     * Parse JSON string and return appropriate type based on content.
     * For arrays: returns List<Object>
     * For objects: returns Map<Object, Object> (always unordered LinkedHashMap)
     * @param json JSON string to parse
     * @return List<Object> for arrays, Map<Object, Object> for objects, or primitive value
     * @throws IOException if parsing fails
     */
    public static Object parseJsonWithTypeHandling(String json) throws IOException {
        JsonNode root = RELAXED_MAPPER.readTree(json);

        if (root.isArray()) {
            List<Object> list = RELAXED_MAPPER.convertValue(root, new TypeReference<List<Object>>() {});
            // Walk list and coerce any map keys in nested objects
            return coerceKeysInList(list);
//            return list;
        } else if (root.isObject()) {
            Map<Object, Object> map = RELAXED_MAPPER.convertValue(root, new TypeReference<Map<Object, Object>>() {});
            // Coerce keys to Integer/Long/Boolean/Double and build Map<Object,Object>
//            return coerceKeysInMap(map);
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                log.info("Map entry: " + entry.getKey() + " -> " + entry.getValue());
                log.info("Map types: " + entry.getKey().getClass().getSimpleName() + " -> " + entry.getValue().getClass().getSimpleName());
            }
            return map;
        } else {
            // Primitive value
            return RELAXED_MAPPER.convertValue(root, Object.class);
        }
    }

    /**
     * Recursively coerce keys in a list (for nested objects within arrays).
     * @param list The list to process
     * @return List with coerced keys in any nested objects
     */
    private static List<Object> coerceKeysInList(List<Object> list) {
        List<Object> result = new java.util.ArrayList<>();
        for (Object item : list) {
            log.info("Processing item: " + item);
            log.info("Item type: " + (item != null ? item.getClass().getSimpleName() : "null"));
            if (item instanceof Map) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) item).entrySet()) {
                    log.info("Map entry: " + entry.getKey() + " -> " + entry.getValue());
                    log.info("Map entry: " + entry.getKey().getClass().getSimpleName() + " -> " + entry.getValue().getClass().getSimpleName());
                }
//                @SuppressWarnings("unchecked")
//                Map<String, Object> mapItem = (Map<String, Object>) item;
//                result.add(coerceKeysInMap(mapItem));
                result.add(item);
            } else if (item instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> listItem = (List<Object>) item;
                result.add(coerceKeysInList(listItem));
            } else {
                result.add(item);
            }
        }
        return result;
    }

    /**
     * Coerce string keys to appropriate types and recursively process nested structures.
     * @param map The map to process
     * @return Map<Object, Object> with coerced keys
     */
//    private static Map<Object, Object> coerceKeysInMap(Map<Object, Object> map) {
//        Map<Object, Object> result = new LinkedHashMap<>(); // Preserves insertion order
//
//        for (Map.Entry<Object, Object> entry : map.entrySet()) {
//            Object stringKey = entry.getKey();
//            Object value = entry.getValue();
//
//            // Coerce the key
//            Object coercedKey = coerceKey(stringKey);
//
//            // Recursively process nested structures
//            Object processedValue;
//            if (value instanceof Map) {
//                @SuppressWarnings("unchecked")
//                Map<String, Object> mapValue = (Map<String, Object>) value;
//                processedValue = coerceKeysInMap(mapValue);
//            } else if (value instanceof List) {
//                @SuppressWarnings("unchecked")
//                List<Object> listValue = (List<Object>) value;
//                processedValue = coerceKeysInList(listValue);
//            } else {
//                processedValue = value;
//            }
//
//            result.put(coercedKey, processedValue);
//        }
//
//        return result;
//    }

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