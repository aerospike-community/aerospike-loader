package com.aerospike.load;

import org.junit.Test;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.List;

public class RelaxedJsonMapperTest {

    @Test
    public void testParseJsonWithUnquotedKeys() throws Exception {
        String json = "{name: 'John', age: 30}";
        JsonNode node = RelaxedJsonMapper.parseJson(json);
        
        assertNotNull(node);
        assertTrue(node.isObject());
        assertEquals("John", node.get("name").asText());
        assertEquals(30, node.get("age").asInt());
    }

    @Test
    public void testParseJsonWithKeyCoercion() throws Exception {
        String json = "{1: 'value1', 2: 'value2', true: 'flag', '3.14': 'pi'}";
        Map<Object, Object> result = RelaxedJsonMapper.parseJsonWithKeyCoercion(json);
        
        assertEquals("value1", result.get(1));
        assertEquals("value2", result.get(2));
        assertEquals("flag", result.get(true));
        assertEquals("pi", result.get(3.14));
    }

    @Test
    public void testParseJsonToList() throws Exception {
        String json = "[1, 2, 'three', true]";
        List<Object> list = RelaxedJsonMapper.parseJsonToList(json);
        
        assertEquals(4, list.size());
        assertEquals(1, list.get(0));
        assertEquals(2, list.get(1));
        assertEquals("three", list.get(2));
        assertEquals(true, list.get(3));
    }

    @Test
    public void testParseJsonToMap() throws Exception {
        String json = "{key1: 'value1', key2: 42, key3: true}";
        Map<String, Object> map = RelaxedJsonMapper.parseJsonToMap(json);
        
        assertEquals("value1", map.get("key1"));
        assertEquals(42, map.get("key2"));
        assertEquals(true, map.get("key3"));
    }

    @Test
    public void testGetFromJsonNode() throws Exception {
        String json = "{name: 'Test', count: 5}";
        JsonNode node = RelaxedJsonMapper.parseJson(json);
        
        assertEquals("Test", RelaxedJsonMapper.getFromJsonNode(node, "name"));
        assertEquals(5, RelaxedJsonMapper.getFromJsonNode(node, "count"));
        assertNull(RelaxedJsonMapper.getFromJsonNode(node, "nonexistent"));
    }

    @Test
    public void testHasField() throws Exception {
        String json = "{name: 'Test', count: 5}";
        JsonNode node = RelaxedJsonMapper.parseJson(json);
        
        assertTrue(RelaxedJsonMapper.hasField(node, "name"));
        assertTrue(RelaxedJsonMapper.hasField(node, "count"));
        assertFalse(RelaxedJsonMapper.hasField(node, "nonexistent"));
    }

    @Test
    public void testJsonNodeToObject() throws Exception {
        String json = "{nested: {value: 123}, array: [1, 2, 3]}";
        JsonNode node = RelaxedJsonMapper.parseJson(json);
        
        Object nestedObj = RelaxedJsonMapper.jsonNodeToObject(node.get("nested"));
        assertTrue(nestedObj instanceof Map);
        
        Object arrayObj = RelaxedJsonMapper.jsonNodeToObject(node.get("array"));
        assertTrue(arrayObj instanceof List);
    }
} 