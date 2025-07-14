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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.ResultCode;

/**
 * 
 * @author Aerospike
 *
 * Main writer class to write data in Aerospike.
 * 
 */
public class AsWriterTask implements Callable<Integer> {

	// File and line info variable.
	private String fileName;
	private int lineNumber;
	private int lineSize;
	
	// Aerospike related variable.
	private AerospikeClient client;
	
	// Data definition related variable
	private HashMap<String, String>	dsvConfigs;
	private MappingDefinition mappingDef;
	private List<String> columns;

	private Parameters params;
	private Counter counters;
	// JSON parsing is now handled by RelaxedJsonMapper

	private static Logger log = LogManager.getLogger(AsWriterTask.class);

	/**
	 * AsWriterTask process given data columns for a record and create Set and Key and Bins.
	 * It writes these Bins to created Key. If its secondary mapping then it will do CDT append
	 * over all created Bins.
	 * 
	 * @param fileName   Name of the data file
	 * @param lineNumber Line number in the file fileName
	 * @param lineSize   Size of the line to keep track of record processed 
	 * @param client     AerospikeClient object
	 * @param columns    List of column separated entries in this lineNumber
	 * @param dsvConfig  Map of DSV configurations
	 * @param mappingDef MappingDefinition of a mapping from config file
	 * @param params     User given parameters
	 * @param counters   Counter for stats
	 * 
	 */
	public AsWriterTask(String fileName, int lineNumber, int lineSize,AerospikeClient client, List<String> columns, 
			HashMap<String, String> dsvConfigs, MappingDefinition mappingDef, Parameters params, Counter counters) {
		
		// Passed to print log error with filename, line number, increment byteprocessed.
		this.fileName = fileName;
		this.lineNumber = lineNumber;
		this.lineSize = lineSize;
		
		this.client = client;

		this.dsvConfigs = dsvConfigs;
		this.mappingDef = mappingDef;
		this.columns = columns;

		this.params = params;
		this.counters = counters;

	}

	/*
	 * Writes a record to the Aerospike Cluster
	 */
	private void writeToAs(Key key, List<Bin> bins) {

		try {
			// Connection could be broken at actual write time.
			if (this.client == null) {
				throw new Exception("Null Aerospike client !!");
			}


			if (bins.isEmpty()) {
				counters.write.noBinsSkipped.getAndIncrement();
				log.trace("No bins to insert");
				return;
			}
			// All bins will have append operation if secondary mapping.
			if (this.mappingDef.secondaryMapping) {
				for (Bin b : bins) {
					client.operate(this.params.writePolicy, key,
							com.aerospike.client.cdt.ListOperation.append(b.name, b.value));
				}
				counters.write.mappingWriteCount.getAndIncrement();
			} else {
				this.client.put(this.params.writePolicy, key, bins.toArray(new Bin[bins.size()]));
				counters.write.bytesProcessed.addAndGet(this.lineSize);
			}
			counters.write.writeCount.getAndIncrement();
			
			log.trace("Wrote line " + lineNumber + " Key: " + key.userKey + " to Aerospike.");

		} catch (AerospikeException ae) {

			handleAerospikeWriteError(ae);
			checkAndAbort();	

		} catch (Exception e) {

			handleWriteError(e);
			checkAndAbort();

		}
	}
	
	private void handleAerospikeWriteError(AerospikeException ae) {

		log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber + "Aerospike Write Error: "
				+ ae.getResultCode());

		if (log.isDebugEnabled()) {
			ae.printStackTrace();
		}

		switch (ae.getResultCode()) {

			case ResultCode.TIMEOUT:
				counters.write.writeTimeouts.getAndIncrement();
				break;
			case ResultCode.KEY_EXISTS_ERROR:
				counters.write.writeKeyExists.getAndIncrement();
				break;
			default:
				//..
		}

		if (!this.mappingDef.secondaryMapping) {
			counters.write.bytesProcessed.addAndGet(this.lineSize);
		}

		counters.write.writeErrors.getAndIncrement();	
	}

	private void handleWriteError(Exception e) {
		log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber + " Write Error: " + e);
		if (log.isDebugEnabled()) {
			e.printStackTrace();
		}
		if (!this.mappingDef.secondaryMapping) {
			counters.write.bytesProcessed.addAndGet(this.lineSize);
		}
		counters.write.writeErrors.getAndIncrement();
	}

	private void checkAndAbort(){
		long errorTotal;
		errorTotal = (counters.write.readErrors.get() + counters.write.writeErrors.get()
						+ counters.write.processingErrors.get());
		if (this.params.abortErrorCount != 0 && this.params.abortErrorCount < errorTotal) {
			System.exit(-1);
		}
	}

	/*
	 * Create Set and Key from provided data for given mappingDef.
	 * Create Bin for given binList in mappingDef.
	 */
	private Key getKeyAndBinsFromDataline(List<Bin> bins) {
		log.debug("processing  File: " + Utils.getFileName(fileName) + "line: " + this.lineNumber);
		Key key = null;

		try {

			validateNColumnInDataline();

			// Set couldn't be null here. Its been validated earlier.
			String set = getSetName();

			key = createRecordKey(this.params.namespace, set);
			
			populateAsBinFromColumnDef(bins);
			
			log.trace("Formed key and bins for line: " + lineNumber + " Key: " + key.userKey + " Bins: "
					+ bins.toString());

		} catch (AerospikeException ae) {

			log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber
					+ " Aerospike Bin processing Error: " + ae.getResultCode());
			handleProcessLineError(ae);

		} catch (Exception e) {

			log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber + " Error: " + e);
			handleProcessLineError(e);

		}
		return key;
	}

	/*
	 * Validate if number of column in data line are same as provided in config file.
	 * Throw exception the more columns are present then given.
	 */
	private void validateNColumnInDataline() throws Exception {
		
		// Throw exception if n_columns(datafile) are more than n_columns(configfile).
		int n_column = Integer.parseInt(dsvConfigs.get(Constants.N_COLUMN));
		if (columns.size() == n_column) {
			return;
		}
		if (columns.size() < n_column) {
			log.warn("File: " + Utils.getFileName(fileName) + " Line: " + lineNumber
					+ " Number of column mismatch:Columns in data file is less than number of column in config file.");
		} else {
			throw new Exception("Column count mismatch at line " + lineNumber);
		}
	}
	
	private void handleProcessLineError(Exception e) {
		if (log.isDebugEnabled()) {
			e.printStackTrace();
		}
		counters.write.processingErrors.getAndIncrement();
		counters.write.bytesProcessed.addAndGet(this.lineSize);
		checkAndAbort();
	}

	
	private String getSetName() {
		MetaDefinition setColumn = this.mappingDef.setColumnDef;

		if (setColumn.staticName != null) {
			return setColumn.staticName;
		}

		String set = null;
		String setRawText = this.columns.get(setColumn.nameDef.columnPos);
		if (setColumn.nameDef.removePrefix != null) {
			if (setRawText != null && setRawText.startsWith(setColumn.nameDef.removePrefix)) {
				set = setRawText.substring(setColumn.nameDef.removePrefix.length());
			}
		} else {
			set = setRawText;
		}
		return set;
	}
	
	private Key createRecordKey(String namespace, String set) throws Exception {
		// Use 'SET' name to create key.
		Key key = null;

		MetaDefinition keyColumn = this.mappingDef.keyColumnDef;
		
		String keyRawText = this.columns.get(keyColumn.nameDef.columnPos);
		
		if (keyRawText == null || keyRawText.trim().length() == 0) {
			counters.write.keyNullSkipped.getAndIncrement();
			throw new Exception("Key value is null in datafile.");
		}

		if ((keyColumn.nameDef.removePrefix != null)
				&& (keyRawText.startsWith(keyColumn.nameDef.removePrefix))) {
			keyRawText = keyRawText.substring(keyColumn.nameDef.removePrefix.length());
		}

		if (keyColumn.nameDef.srcType == SrcColumnType.INTEGER) {
			Long integer = Long.parseLong(keyRawText);
			key = new Key(namespace, set, integer);
		} else {
			key = new Key(namespace, set, keyRawText);
		}

		return key;
	}
	
	private void populateAsBinFromColumnDef(List<Bin> bins) {
		for (BinDefinition binColumn : this.mappingDef.binColumnDefs) {
			Bin bin = null;
			String binName = null;
			String binRawValue = null;

			// Get binName.
			if (binColumn.staticName != null) {
				binName = binColumn.staticName;
			} else if (binColumn.nameDef != null) {
				binName = this.columns.get(binColumn.nameDef.columnPos);
			}

			// Get BinValue.
			if (binColumn.staticValue != null) {

				binRawValue = binColumn.staticValue;
				bin = new Bin(binName, binRawValue);

			} else if (binColumn.valueDef != null) {

				binRawValue = getbinRawValue(binColumn);
				if (binRawValue == null || binRawValue.equals("")) {
					continue;
				}

				switch (binColumn.valueDef.srcType) {

					case INTEGER:
						bin = createBinForInteger(binName, binRawValue);
						break;
					case FLOAT:
						bin = createBinForFloat(binName, binRawValue);
						break;
					case STRING:
						bin = createBinForString(binName, binRawValue);
						break;
					case JSON:
						/*
						 * JSON could take any valid JSON. There are two type of JSON:
						 * JsonArray: this can be used to insert List (Any generic JSON list)
						 * JsonObj: this can be used to insert Map (Any generic JSON object)
						 */
						bin = createBinForJson(binName, binRawValue);
						break;
					case GEOJSON:
						bin = createBinForGeoJson(binName, binRawValue);
						break;
					case BLOB:
						bin = createBinForBlob(binColumn, binName, binRawValue);
						break;
					case TIMESTAMP:
						bin = createBinForTimestamp(binColumn, binName, binRawValue);
						break;
					default:
						//....
					}
			}

			if (bin != null) {	
				bins.add(bin);
			}
		}
	}

	private String getbinRawValue(BinDefinition binColumn) {
		/*
		 * User may want to store the time when record is written in Aerospike.
		 * Assign system_time to binvalue. This bin will be written as part of
		 * record.
		 */
		if (binColumn.valueDef.columnName != null
				&& binColumn.valueDef.columnName.toLowerCase().equals(Constants.SYSTEM_TIME)) {

			SimpleDateFormat sdf =
				new SimpleDateFormat(binColumn.valueDef.encoding);    // dd/MM/yyyy
			Date now = new Date();
			return sdf.format(now);
		}

		
		String binRawValue = this.columns.get(binColumn.valueDef.columnPos);

		if ((binColumn.valueDef.removePrefix != null)
				&& (binRawValue != null && binRawValue.startsWith(binColumn.valueDef.removePrefix))) {
			binRawValue =
				binRawValue.substring(binColumn.valueDef.removePrefix.length());
		}
		return binRawValue;
	}
	
	private Bin createBinForInteger(String binName, String binRawValue) {

		try {
			// Server stores all integer type data in 64bit so use long.
			Long integer = Long.parseLong(binRawValue);

			return new Bin(binName, integer);

		} catch (Exception pi) {

			log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber
					+ " Integer/Long Parse Error: " + pi);
			return null;

		}
	}

	private Bin createBinForFloat(String binName, String binRawValue) {

		try {
			// parse as a double to get greater precision
			double binfloat = Double.parseDouble(binRawValue);

			return new Bin(binName, binfloat);

		} catch (Exception e) {
			log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber
					+ " Floating number Parse Error: " + e);
			return null;
		}

	}

	private Bin createBinForString(String binName, String binRawValue) {
		return new Bin(binName, binRawValue);
	}
	
	private Bin createBinForJson(String binName, String binRawValue) {
		try {
			log.debug(binRawValue);

			JsonNode jsonNode = RelaxedJsonMapper.parseJson(binRawValue);

			if (jsonNode.isArray()) {
				List<Object> jsonArray = RelaxedJsonMapper.parseJsonToList(binRawValue);
				return new Bin(binName, jsonArray);
			} else {
				Object jsonObj = RelaxedJsonMapper.jsonNodeToObject(jsonNode);

				if (this.params.unorderdMaps) {
					if (jsonObj instanceof Map) {
						return  new Bin(binName, (Map<?, ?>) jsonObj);
					}
					return new Bin(binName, jsonObj.toString());
				}

				if (jsonObj instanceof Map) {
					TreeMap<Object, Object> sortedMap = new TreeMap<>();
					sortedMap.putAll((Map<?, ?>) jsonObj);
					return  new Bin(binName, sortedMap);
				}
				
				return new Bin(binName, jsonObj.toString());
			}

		} catch (IOException e) {
			log.error("Failed to parse JSON: " + e);
			return null;
		}

	}
	
	private Bin createBinForGeoJson(String binName, String binRawValue) {
		try {
			return new Bin(binName, Value.getAsGeoJSON(binRawValue));
		} catch (Exception e) {
			log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber
					+ " GeoJson Parse Error: " + e);
			return null;
		}
	}
	
	private Bin createBinForBlob(BinDefinition binColumn, String binName, String binRawValue) {
		try {
			if ((binColumn.valueDef.dstType.equals(DstColumnType.BLOB))
					&& (binColumn.valueDef.encoding.equalsIgnoreCase(Constants.HEX_ENCODING))) {
				return new Bin(binName, this.toByteArray(binRawValue));
			}
		} catch (Exception e) {
			log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber
					+ " Blob Parse Error: " + e);
			return null;
		}

		return null;
	}

	private Bin createBinForTimestamp(BinDefinition binColumn, String binName, String binRawValue) {

		if (! binColumn.valueDef.dstType.equals(DstColumnType.INTEGER)) {
			return new Bin(binName, binRawValue);
		}

		DateFormat format = new SimpleDateFormat(binColumn.valueDef.encoding);

		try {

			Date formatDate = format.parse(binRawValue);
			long milliSecondForDate = formatDate.getTime() - this.params.timeZoneOffset;

			if (!(binColumn.valueDef.encoding.contains(".SSS")
					&& binName.equals(Constants.SYSTEM_TIME))) {
				// We need time in milliseconds so no need to change it to milliseconds.
				milliSecondForDate = milliSecondForDate / 1000;
			}

			log.trace("Date format: " + binRawValue + " in seconds: " + milliSecondForDate);

			return new Bin(binName, milliSecondForDate);

		} catch (java.text.ParseException e) {
			e.printStackTrace();
			return null;
		}

	}
	
	private byte[] toByteArray(String s) {

		if ((s.length() % 2) != 0) {
			log.error("blob exception: " + s);
			throw new IllegalArgumentException("Input hex formated string must contain an even number of characters.");
		}

		int len = s.length();
		byte[] data = new byte[len / 2];

		try {
			for (int i = 0; i < len; i += 2) {
				data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
			}
		} catch (Exception e) {
			log.error("blob exception: " + e);
		}
		return data;
	}

	private boolean exceedingThroughput() {
		long transactions;
		long timeLapse;
		long throughput;

		transactions = counters.write.writeCount.get()
				+ counters.write.mappingWriteCount.get()
				+ counters.write.writeErrors.get();


		timeLapse = (System.currentTimeMillis() - counters.write.writeStartTime) / 1000L;
		
		if (timeLapse > 0) {
			throughput = transactions / timeLapse;

			if (throughput > params.maxThroughput) {
				return true;
			}
		}
		return false;
	}
	public Integer call() throws Exception {

		List<Bin> bins = new ArrayList<Bin>();


		try {

			counters.write.processingCount.getAndIncrement();

			Key key = getKeyAndBinsFromDataline(bins);

			if (key != null) {
				writeToAs(key, bins);
				bins.clear();

				if (params.maxThroughput == 0) {
					return 0;
				}

				while(exceedingThroughput()) {
					Thread.sleep(20);
				}

				return 0;
			}

		} catch (Exception e) {

			log.error("File: " + Utils.getFileName(this.fileName) + " Line: " + lineNumber + " Parsing Error: " + e);
			log.debug(e);
		}

		return 0;

	}
}
