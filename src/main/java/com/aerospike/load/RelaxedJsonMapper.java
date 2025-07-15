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
     * For objects: returns Map<Object, Object> (ordered or unordered based on parameter)
     * @param json JSON string to parse
     * @param unorderedMaps if true, return unordered maps; if false, return TreeMap for objects
     * @return List<Object> for arrays, Map<Object, Object> for objects, or primitive value
     * @throws IOException if parsing fails
     */
    public static Object parseJsonWithTypeHandling(String json, boolean unorderedMaps) throws IOException {
        JsonNode node = RELAXED_MAPPER.readTree(json);
        return convertJsonNodeWithKeyCoercion(node, unorderedMaps);
    }

    /**
     * Recursively convert a JsonNode to Java objects with key coercion applied.
     * @param node The JsonNode to convert
     * @param unorderedMaps if true, return unordered maps; if false, return TreeMap for objects
     * @return The converted Java object with coerced keys
     */
    private static Object convertJsonNodeWithKeyCoercion(JsonNode node, boolean unorderedMaps) {
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
            // Process array elements recursively
            java.util.List<Object> list = new java.util.ArrayList<>();
            for (JsonNode element : node) {
                list.add(convertJsonNodeWithKeyCoercion(element, unorderedMaps));
            }
            return list;
        } else if (node.isObject()) {
            // Process object with key coercion and recursive value processing
            Map<Object, Object> map = unorderedMaps ? 
                new LinkedHashMap<>() : new LinkedHashMap<>(); // Start with LinkedHashMap, convert to TreeMap later if needed
            
            node.fields().forEachRemaining(entry -> {
                String stringKey = entry.getKey();
                JsonNode valueNode = entry.getValue();
                
                // Coerce the key
                Object coercedKey = coerceKey(stringKey);
                
                // Recursively process the value
                Object processedValue = convertJsonNodeWithKeyCoercion(valueNode, unorderedMaps);
                
                map.put(coercedKey, processedValue);
            });
            
            // If ordered maps are requested, try to create a TreeMap
            if (!unorderedMaps) {
                try {
                    java.util.TreeMap<Object, Object> sortedMap = new java.util.TreeMap<>();
                    sortedMap.putAll(map);
                    return sortedMap;
                } catch (ClassCastException e) {
                    // Keys not comparable, return unordered map
                    return map;
                }
            }
            
            return map;
        }
        
        return node.toString();
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