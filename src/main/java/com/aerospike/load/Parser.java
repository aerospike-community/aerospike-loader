/*******************************************************************************
 * Copyright 2022 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.load;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;

/**
 * Parser class to parse different schema/(data definition) and data files.
 *
 */
public class Parser {
	private static Logger log = LogManager.getLogger(Parser.class);

	/**
	 * Process column definitions in JSON formated file and create two lists for metadata and
	 * bindata and one list for metadataLoader
	 * 
	 * @param  configFile     Config/schema/definition file name
	 * @param  dsvConfigs     DSV configuration for loader to use, given in config file (version, n_columns, delimiter...)
	 * @param  mappingDefs    List of schema/definitions for all mappings. primary + secondary mappings.
	 * @param  params         User given parameters
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static boolean parseJSONColumnDefinitions(File configFile, HashMap<String, String> dsvConfigs,	List<MappingDefinition> mappingDefs) {
		FileReader fr = null;
		try {
			fr = new FileReader(configFile);
			JsonNode jobj = RelaxedJsonMapper.parseJson(fr);

			if (jobj == null) {
				log.error("Empty config File.");
				if (fr != null)
					fr.close();
				return false;
			} else {
				log.debug("Config file contents: " + RelaxedJsonMapper.jsonNodeToString(jobj));
			}

			/*
			 * Get Metadata, DSV_CONFIG parameters. version, (n_columns, delimiter, header_exist..)
			 * Get mapping definition
			 */
			if (getUpdateDsvConfig(jobj, dsvConfigs) == false
					|| getUpdateMappingColumnDefs(jobj, mappingDefs) == false) {
				if (fr != null)
					fr.close();
				return false;
			}
			
		} catch (IOException ie) {
			log.error("File: " + Utils.getFileName(configFile.getName()) + " Config i/o Error: " + ie.toString());
			if (log.isDebugEnabled()) {
				ie.printStackTrace();
			}
		} catch (Exception e) {
			log.error("File: " + Utils.getFileName(configFile.getName()) + " Config unknown Error: " + e.toString());
			if (log.isDebugEnabled()) {
				e.printStackTrace();
			}
		} finally {
			if (fr == null) {
				return true;
			}
			try {
				fr.close();
			} catch (IOException fce) {
				log.error("File: " + Utils.getFileName(configFile.getName()) + " File reader closing error: "
						+ fce.toString());
				if (log.isDebugEnabled()) {
					fce.printStackTrace();
				}
			}
		}
		return true;
	}

	/*
	 * Parse Dsv_configs from config file.
	 */
	private static boolean getUpdateDsvConfig(JsonNode jobj, HashMap<String, String> dsvConfigs) {
		Object obj = null;

		// Get Metadata of loader (update dsvConfigs)
		if ((obj = RelaxedJsonMapper.getFromJsonNode(jobj, Constants.VERSION)) == null) {
			log.error("\"" + Constants.VERSION + "\"  Key is missing in config file.");
			return false;
		}
		dsvConfigs.put(Constants.VERSION, obj.toString());

		
		// Get DSV_CONFIG parameters. (n_columns, delimiter, header_exist..)
		if ((obj = RelaxedJsonMapper.getFromJsonNode(jobj, Constants.DSV_CONFIG)) == null) {
			log.error("\"" + Constants.DSV_CONFIG + "\"  Key is missing in config file.");
			return false;
		}
		JsonNode dsvConfigObj = jobj.get(Constants.DSV_CONFIG);

		if ((obj = RelaxedJsonMapper.getFromJsonNode(dsvConfigObj, Constants.N_COLUMN)) == null) {
			log.error("\"" + Constants.N_COLUMN + "\"  Key is missing in config file.");
			return false;
		}
		dsvConfigs.put(Constants.N_COLUMN, obj.toString());

		// Delimiter and Header_exist config are optional.
		if ((obj = RelaxedJsonMapper.getFromJsonNode(dsvConfigObj, Constants.DELIMITER)) != null)
			dsvConfigs.put(Constants.DELIMITER, obj.toString());

		if ((obj = RelaxedJsonMapper.getFromJsonNode(dsvConfigObj, Constants.HEADER_EXIST)) != null)
			dsvConfigs.put(Constants.HEADER_EXIST, obj.toString());
		
		return true;
	}

	/*
	 * Parse all mapping given in config file.
	 */
	private static boolean getUpdateMappingColumnDefs(JsonNode jobj, List<MappingDefinition> mappingDefs) throws Exception {
		Object obj = null;
		JsonNode mappings;
		if ((obj = RelaxedJsonMapper.getFromJsonNode(jobj, Constants.MAPPINGS)) != null) {
			mappings = jobj.get(Constants.MAPPINGS);
			if (mappings.isArray()) {
				for (JsonNode mappingObj : mappings) {
					MappingDefinition md = getMappingDef(mappingObj);
					if (md != null) {
						mappingDefs.add(md);
					} else {
						log.error("Error in parsing mappingdef: " + RelaxedJsonMapper.jsonNodeToString(mappingObj));
						return false;
					}
				}
			}
		}
		return true;
	}
	
	private static Object getFromJsonObject(JsonNode jobj, String key) {
		return RelaxedJsonMapper.getFromJsonNode(jobj, key);
	}

	/*
	 * Parse mapping definition from config file to get mappingDef
	 * This will have (secondary_mapping_ keyDefinition, setDefinition, binDefinition).  
	 */
	private static MappingDefinition getMappingDef(JsonNode mappingObj) throws Exception {
		
		boolean secondary_mapping = false;
		MetaDefinition keyColumnDef = null;
		MetaDefinition setColumnDef = null;
		List<BinDefinition> binColumnDefs = new ArrayList<BinDefinition>();

		Object obj;

		// Get secondary_mapping info.
		obj = getFromJsonObject(mappingObj, Constants.SECONDARY_MAPPING);
		if (obj != null) {
			secondary_mapping = Boolean.valueOf((String) obj);
		}

		if ((obj = getFromJsonObject(mappingObj, Constants.KEY)) != null) {
			keyColumnDef = getMetaDefs(mappingObj.get(Constants.KEY), Constants.KEY);
		} else {
			log.error("\"" + Constants.KEY + "\"  Key is missing in mapping. Mapping: " + RelaxedJsonMapper.jsonNodeToString(mappingObj));
			return null;
		}


		if ((obj = getFromJsonObject(mappingObj, Constants.SET)) == null) {
			log.error("\"" + Constants.SET + "\"  Key is missing in mapping. Mapping: " + RelaxedJsonMapper.jsonNodeToString(mappingObj));
			return null;
		} else if (obj instanceof String) {
			setColumnDef = new MetaDefinition(obj.toString(), null);
		} else {
			setColumnDef = getMetaDefs(mappingObj.get(Constants.SET), Constants.SET);
		}

		if ((obj = getFromJsonObject(mappingObj, Constants.BINLIST)) != null) {
			JsonNode binObjList = mappingObj.get(Constants.BINLIST);
			if (binObjList.isArray()) {
				for (JsonNode binObj : binObjList) {
					BinDefinition binDef = getBinDefs(binObj);
					if (binDef != null) {
						binColumnDefs.add(binDef);
					} else {
						log.error("Error in parsing binDef: " + RelaxedJsonMapper.jsonNodeToString(binObj));
						return null;
					}
				}
			}
		} else {
			log.error("\"" + Constants.BINLIST + "\"  Key is missing in mapping. Mapping: " + mappingObj.toString());
			return null;
		}

		return new MappingDefinition(secondary_mapping, keyColumnDef, setColumnDef, binColumnDefs);
	}

	/*
	 * Parsing Meta definition(for Set or Key) from config file and populate metaDef object.
	 */
	private static MetaDefinition getMetaDefs(JsonNode jobj, String jobjName) {
		// Parsing Key, Set definition
		ColumnDefinition valueDef = new ColumnDefinition(-1, null, null, null, null, null, null);
		
		if ((jobj.get(Constants.COLUMN_POSITION)) != null) {
			
			valueDef.columnPos = (Integer.parseInt(jobj.get(Constants.COLUMN_POSITION).toString()) - 1);
			
		} else if ((jobj.get(Constants.COLUMN_NAME)) != null) {
			
			valueDef.columnName = jobj.get(Constants.COLUMN_NAME).asText();

		} else {
			log.error("Column_name or pos info is missing. Specify proper key/set mapping in config file for: " + jobjName + ":"
					+ RelaxedJsonMapper.jsonNodeToString(jobj));
		}

		JsonNode typeNode = jobj.get(Constants.TYPE);
		valueDef.setSrcType(typeNode != null ? typeNode.asText() : null);

		// Default set type is 'string'. what is default key type?
		if (Constants.SET.equalsIgnoreCase(jobjName) && valueDef.srcType == null) {
			valueDef.setSrcType("string");
		}

		// Get prefix to remove. Prefix will be removed from data
		JsonNode prefixNode = jobj.get(Constants.REMOVE_PREFIX);
		if (prefixNode != null) {
			valueDef.removePrefix = prefixNode.asText();
		}
		
		return new MetaDefinition(null, valueDef);
	}

	/*
	 * Parsing Bin definition from config file and populate BinDef object
	 */
	private static BinDefinition getBinDefs(JsonNode jobj) {
		/*
		 * Sample Bin object
		 * {"name": "age", "value": {"column_name": "age", "type" : "integer"} }
		 */

		Object obj;

		// Parsing Bin name
		ColumnDefinition nameDef = new ColumnDefinition(-1, null, null, null, null, null, null);
		String staticBinName = null;
		
		JsonNode nameNode = jobj.get(Constants.NAME);
		if (nameNode == null) {
			log.error(Constants.NAME + " key is missing object: " + RelaxedJsonMapper.jsonNodeToString(jobj));
			return null;
		} else if (nameNode.isTextual()) {
			staticBinName = nameNode.asText();
		} else {
			JsonNode posNode = nameNode.get(Constants.COLUMN_POSITION);
			if (posNode != null) {
				nameDef.columnPos = (Integer.parseInt(posNode.asText()) - 1);
			} else {
				JsonNode colNameNode = nameNode.get(Constants.COLUMN_NAME);
				if (colNameNode != null) {
					nameDef.columnName = colNameNode.asText();
				} else {
					log.error("Column_name or pos info is missing. Specify proper bin name mapping in config file for: " + RelaxedJsonMapper.jsonNodeToString(jobj));
				}
			}
		}

		// Parsing Bin value
		ColumnDefinition valueDef = new ColumnDefinition(-1, null, null, null, null, null, null);
		String staticBinValue = null;

		JsonNode valueNode = jobj.get(Constants.VALUE);
		if (valueNode == null) {
			log.error(Constants.VALUE + " key is missing in bin object:" + RelaxedJsonMapper.jsonNodeToString(jobj));
			return null;
		} else if (valueNode.isTextual()) {
			staticBinValue = valueNode.asText();
		} else {
			JsonNode posNode = valueNode.get(Constants.COLUMN_POSITION);
			if (posNode != null) {
				valueDef.columnPos = (Integer.parseInt(posNode.asText()) - 1);
			} else {
				JsonNode colNameNode = valueNode.get(Constants.COLUMN_NAME);
				if (colNameNode != null) {
					valueDef.columnName = colNameNode.asText();

				} else {
					log.error("Column_name or pos info is missing. Specify proper bin value mapping in config file for: " + RelaxedJsonMapper.jsonNodeToString(jobj));
				}
			}
			
			JsonNode typeNode = valueNode.get(Constants.TYPE);
			valueDef.setSrcType(typeNode != null ? typeNode.asText() : null);
			if (valueDef.srcType == null) {
				log.error(Constants.TYPE + " key is missing in bin object: " + RelaxedJsonMapper.jsonNodeToString(jobj));
			}
			
			JsonNode dstTypeNode = valueNode.get(Constants.DST_TYPE);
			valueDef.setDstType(dstTypeNode != null ? dstTypeNode.asText() : null);
			
			JsonNode encodingNode = valueNode.get(Constants.ENCODING);
			valueDef.encoding = encodingNode != null ? encodingNode.asText() : null;
			
			JsonNode prefixNode = valueNode.get(Constants.REMOVE_PREFIX);
			valueDef.removePrefix = prefixNode != null ? prefixNode.asText() : null;
		}
		
		return new BinDefinition(staticBinName, staticBinValue, nameDef, valueDef); 
	}

	/**
	 * Parses the line into list of raw column string values
	 * Format of data should follow one rule: Data should not contain delimiter as part of data.
	 * 
	 * @param  line: line from the data file with DSV formated
	 * @return List: List of column entries from line
	 */
	public static List<String> getDSVRawColumns(String line, String delimiter){
		if(line==null || line.trim().length()==0){
			return null;
		}
		List<String> store = new ArrayList<String>();
		StringBuilder curVal = new StringBuilder();
		boolean inquotes = false;
		boolean prevDelimiter = false;
		char[] delimiterChars = delimiter.toCharArray();
		int delimiterIndex = 0;
		for (int i=0; i<line.length(); i++) {
			
			char ch = line.charAt(i);
			
			// Trim white space and tabs after delimiter
			if (prevDelimiter) {
				if (Character.isWhitespace(ch)) {
					continue;
				} else {
					prevDelimiter = false;
				}
			}
			
			if (ch == delimiterChars[delimiterIndex] && !(inquotes)) {
				if (delimiterIndex == delimiterChars.length-1){
					prevDelimiter = true;
					// Trim will remove all whitespace character from end of string or
					// after ending double quotes
					store.add(curVal.toString().trim());
					curVal = new StringBuilder();
					delimiterIndex = 0;
					inquotes = false;
				}
				else{
					delimiterIndex++;
				}
				continue;
			}
			if (delimiterIndex > 0){
				// Appending partial delimiters read till now
				curVal.append(Arrays.copyOfRange(delimiterChars, 0, delimiterIndex));
				delimiterIndex = 0;

				if (ch == delimiterChars[delimiterIndex]) {
					if (delimiterIndex == delimiterChars.length-1){
						prevDelimiter = true;
						// Trim will remove all whitespace character from end of string or
						// after ending double quotes
						store.add(curVal.toString().trim());
						curVal = new StringBuilder();
						delimiterIndex = 0;
						inquotes = false;
					}
					else{
						delimiterIndex++;
					}
					continue;
				}
			}
			if (inquotes) {
				if (ch== Constants.DOUBLE_QUOTE_DELEMITER) {
					inquotes = false;
				}
				else {
					curVal.append(ch);
				}
			}
			else {
				
				if (ch == Constants.DOUBLE_QUOTE_DELEMITER) {
					inquotes = true;
					if (curVal.length()>0) {
						inquotes = false;
						//If this is the second quote in a value, add a quote
						//this is for the double quote in the middle of a value
						curVal.append('\"');
					}
				}
				else{
					curVal.append(ch);
				}
			}
		}
		store.add(curVal.toString().trim());
		return store;
	}
	
}
