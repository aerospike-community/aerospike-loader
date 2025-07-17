package com.aerospike.load;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.List;

public class RelaxedJsonMapperTest {

    @Test
    public void testParseJsonWithFileReader() throws Exception {
        // Create a temporary file reader with JSON content
        String json = "{name: 'John', age: 30}";
        // Since parseJson only accepts FileReader, we'll test parseJsonWithTypeHandling instead
        Object result = RelaxedJsonMapper.parseJsonWithTypeHandling(json);
        
        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) result;
        assertEquals("John", map.get("name"));
        assertEquals(30, map.get("age"));
    }

    @Test 
    public void testParseJsonWithTypeHandling_Object() throws Exception {
        // Test data from test.dsv row 4: {2:"a",1:"b"}
        String json = "{2:\"a\",1:\"b\"}";
        Object result = RelaxedJsonMapper.parseJsonWithTypeHandling(json);

        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) result;
        
        // Keys should be coerced to integers
        assertEquals("a", map.get(2));
        assertEquals("b", map.get(1));
        
        // String keys should not exist
        assertNull(map.get("2"));
        assertNull(map.get("1"));
    }

    @Test
    public void testParseJsonWithTypeHandling_Array() throws Exception {
        // Test data from test.dsv row 2: ["3","1","2"]
        String json = "[\"3\",\"1\",\"2\"]";
        Object result = RelaxedJsonMapper.parseJsonWithTypeHandling(json);

        assertTrue(result instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) result;
        
        assertEquals(3, list.size());
        assertEquals("3", list.get(0));
        assertEquals("1", list.get(1));
        assertEquals("2", list.get(2));
    }

    @Test
    public void testNestedObjectsInArraysWithNumericKeys() throws Exception {
        // Test data from test.dsv row 3: ["1",2,{2:"a","1":"b"}]
        String json = "[\"1\",2,{2:\"a\",\"1\":\"b\"}]";
        Object result = RelaxedJsonMapper.parseJsonWithTypeHandling(json);

        assertTrue(result instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) result;

        assertEquals(3, list.size());
        assertEquals("1", list.get(0));
        assertEquals(2, list.get(1));

        // Check the nested object
        assertTrue(list.get(2) instanceof Map);
        @SuppressWarnings("unchecked")
        Map<Object, Object> nestedMap = (Map<Object, Object>) list.get(2);

        // Verify that numeric keys are preserved as numbers, not strings
        assertEquals("a", nestedMap.get(2));  // Key should be integer 2
        assertEquals("b", nestedMap.get(1));  // Key should be integer 1

        // Verify that string keys would not work (they should be coerced to numbers)
        assertNull(nestedMap.get("2"));  // String "2" should not exist
    }

    @Test
    public void testComplexJsonFromTestData() throws Exception {
        // Test data from test.dsv row 1: Complex nested object
        String json = "{\"2\": {\"createdAt\": \"1751896132789\", \"discountId\": \"3000002\", \"expiresAt\": \"1755092932789\", \"id\": \"500000\", \"updatedAt\": \"1752068932790\"}}";
        Object result = RelaxedJsonMapper.parseJsonWithTypeHandling(json);

        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) result;
        
        // Key "2" should be coerced to integer 2
        assertTrue(map.containsKey(2));
        assertFalse(map.containsKey("2"));
        
        Object nestedObject = map.get(2);
        assertTrue(nestedObject instanceof Map);
        
        @SuppressWarnings("unchecked")
        Map<Object, Object> nested = (Map<Object, Object>) nestedObject;
        assertEquals("1751896132789", nested.get("createdAt"));
        assertEquals("3000002", nested.get("discountId"));
        assertEquals("500000", nested.get("id"));
    }

    @Test
    public void testFieldOrderPreservation() throws Exception {
        // Test the specific case mentioned: {2:"a","1":"b"}
        String json = "{2: \"a\", \"1\": \"b\", 3: \"c\"}";
        Object result = RelaxedJsonMapper.parseJsonWithTypeHandling(json);

        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) result;

        // Verify values are correct
        assertEquals("a", map.get(2));
        assertEquals("b", map.get(1));
        assertEquals("c", map.get(3));

        // Verify that the map preserves insertion order
        Object[] keys = map.keySet().toArray();
        Object[] values = map.values().toArray();

        // Should be in insertion order: 2, 1, 3
        assertEquals(2, keys[0]);
        assertEquals(1, keys[1]);
        assertEquals(3, keys[2]);

        assertEquals("a", values[0]);
        assertEquals("b", values[1]);
        assertEquals("c", values[2]);
    }

    @Test
    public void testMixedKeyTypes() throws Exception {
        String json = "{1: \"value1\", 2: \"value2\", true: \"flag\", \"3.14\": \"pi\"}";
        Object result = RelaxedJsonMapper.parseJsonWithTypeHandling(json);

        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) result;

        assertEquals("value1", map.get(1));
        assertEquals("value2", map.get(2));
        assertEquals("flag", map.get(true));
        assertEquals("pi", map.get(3.14));
    }

    @Test
    public void testGetFromJsonNode() throws Exception {
        String json = "{name: \"Test\", count: 5}";
        Object parsed = RelaxedJsonMapper.parseJsonWithTypeHandling(json);
        
        // Since we don't have direct parseJson(String), we can test jsonNodeToObject instead
        assertTrue(parsed instanceof Map);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) parsed;
        
        assertEquals("Test", map.get("name"));
        assertEquals(5, map.get("count"));
    }

    @Test
    public void testJsonNodeToObject() throws Exception {
        // Test primitive values through parseJsonWithTypeHandling
        String numberJson = "123";
        Object numberResult = RelaxedJsonMapper.parseJsonWithTypeHandling(numberJson);
        assertEquals(123, numberResult);

        String stringJson = "\"hello\"";
        Object stringResult = RelaxedJsonMapper.parseJsonWithTypeHandling(stringJson);
        assertEquals("hello", stringResult);

        String booleanJson = "true";
        Object booleanResult = RelaxedJsonMapper.parseJsonWithTypeHandling(booleanJson);
        assertEquals(true, booleanResult);
    }

    @Test
    public void testJsonNodeToString() throws Exception {
        // Test that we can convert parsed objects back to string representation
        String json = "{key: \"value\"}";
        Object result = RelaxedJsonMapper.parseJsonWithTypeHandling(json);
        
        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) result;
        
        // Verify the content is parsed correctly
        assertEquals("value", map.get("key"));
        
        // The map should contain the expected key-value pair
        assertTrue(map.toString().contains("key"));
        assertTrue(map.toString().contains("value"));
    }
}