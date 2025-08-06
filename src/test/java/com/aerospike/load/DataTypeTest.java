package com.aerospike.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.github.stefanbirkner.systemlambda.SystemLambda;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.cdt.MapOrder;

enum BinType {
	INTEGER, STRING, BLOB, LIST, MAP, JSON, TIMESTAMP;
}


/**
 * @author jyoti
 *
 */
public class DataTypeTest {
	
	String host = "127.0.0.1";
	String port = "3100";
	String ns = "test";
	String set = null;
	MapOrder expectedMapOrder = MapOrder.KEY_ORDERED;
	//String config = "src/test/resources/allDatatypeCsv.json";
	String error_count = "0";
	String write_action = "update";
	String timeout = "10";
	String rootDir = "src/test/resources/";
	//String configFile = "";
	String dataFile = "";
	String testSchemaFile = "src/test/resources/testSchema.json";
	// String dataFile = "src/test/resources/data.csv";
	String log = "aerospike-load.log";
	JSONObject testSchema = null;
	AerospikeClient client;

	@Before
	public void setUp() {

		try {
			client = new AerospikeClient(host, Integer.parseInt(port));
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
		testSchema = parseConfigFile(testSchemaFile);
	}

	@After
	public void tearDown() {
		client.close();
	}
	
	public List<List<String>> parseDataFile(String dataFile) {
		BufferedReader br = null;
		String delimiter = ",";
		List<List<String>> recordDataList = new ArrayList<List<String>>();
		try{
			String curLine;
			br = new BufferedReader(new FileReader(dataFile));
			List<String> binDataList = null;
			while ((curLine = br.readLine()) != null) {
				binDataList = Parser.getDSVRawColumns(curLine, delimiter);
			}
			if (binDataList  != null) {
				recordDataList.add(binDataList);
			}
		} catch (IOException e) {
			// Print error
		}
		return recordDataList;
	}
	
	public JSONObject parseConfigFile(String configFile) {
		JSONParser parser = new JSONParser();
		JSONObject jsonObject = null;
		try{
			Object obj = parser.parse(new FileReader(configFile));
			jsonObject = (JSONObject) obj;
		} catch (IOException e) {
			// Print error/abort/skip
		} catch (ParseException e) {
			// throw error/abort test/skip/test
		}
		return jsonObject;
	}

        @Test
	public void testInvalidConfig() throws Exception {
		System.out.println("TestInvalidConfig: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_string");

		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataString.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);

		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "/non/existing/config.json", dataFile});
		});

		// Validate loaded data
		String dstType = null;
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);

		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(-1, exitCode, "Unexpected exit code");

		System.out.println("TestInvalidConfig: Complete");
	}

	// String type data validation
	@Test
	public void testValidateString() throws Exception {
		System.out.println("TestValidateString: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_string");

		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataString.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);

		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configString.json", dataFile});
		});

		// Validate loaded data
		String dstType = null;
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);
		
		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestValidateString: Complete");
	}

	//Integer type data validation
	@Test
	public void testValidateInteger() throws Exception {
		System.out.println("TestValidateInteger: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_integer");


		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataInt.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);  
		
		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configInt.json", dataFile});
		});

		// Validate loaded data
		String dstType = null;
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);
		
		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestValidateInteger: Complete");
	}

	//Utf8 string type data validation
	//@Test
	public void testValidateStringUtf8() throws Exception {
		System.out.println("TestValidateStringUtf8: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_utf8");


		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataUtf8.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);
		
		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configUtf8.json", dataFile});
		});

		// Validate loaded data
		String dstType = null;
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);
		
		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestValidateStringutf8: Complete");
	}

	//timestamp type data validation
	//@Test
	public void testValidateTimestampInteger() throws Exception {
		System.out.println("TestValidateTimestampInteger: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_date");

		
		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataDate.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);
		
		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configDate.json", dataFile});
		});

		// Validate loaded data
		String dst_type = "integer";	
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dst_type);
		boolean error = getError(log);
			
		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestValidateTimestampInteger: Complete");
	}

	//Blob type data validation
	//@Test
	public void testValidateBlob() throws Exception {
		System.out.println("TestValidateBlob: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_blob");

		
		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataBlob.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);

		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configBlob.json", dataFile});
		});

		// Validate loaded data
		String dstType = "blob";
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);
		
		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestValidateBlob: Complete");
	}

	//List type data validation
	@Test
	public void testValidateList() throws Exception {
		System.out.println("TestValidateList: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_list");

		
		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataList.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);
		
		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configList.json", dataFile});
		});

		// Validate loaded data
		String dstType = "list";
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);

		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestValidateList: Complete");
	}

	//Map type data validation
	@Test
	public void testValidateMap() throws Exception {
		System.out.println("TestValidateMap: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_map");

		
		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataMap.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);
		
		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configMap.json", dataFile});
		});

		// Validate loaded data
		String dstType = "map";
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);

		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestValidateMap: Complete");
	}

	//JSON type data validation
	@Test
	public void testValidateJSON() throws Exception {
		System.out.println("TestValidateJSON: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}

		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_json");

		
		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataJson.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);
		
		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-v", "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configJson.json", dataFile});
		});

		// Validate loaded data
		String dstType = "json";
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);

		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestValidateJSON: Complete");
	}

	//Multiple data type insert
	//@Test
	public void testAllDatatype() throws Exception {
		System.out.println("TestAllDatatype: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}

		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_alltype");

		
		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "configAllDataType.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);
		
		// Run Aerospike loader
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configAllDataType.json", dataFile});
		});

		boolean error = getError(log);

		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("TestAllDatatype: Complete");
	}

	//Dynamic bin name
	//@Test
	public void testDynamicBinName() throws Exception {
		System.out.println("Test Dynamic BinName: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}

		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configDynamicBinName.json", "src/test/resources/dataDynamicBin.csv"});
		});


		boolean error = getError(log);

		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("Test Dynamic BinName: Complete");
	}

	//Static binName
	//@Test
	public void testStaticBinName() throws Exception {
		System.out.println("Test static BinName: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}

		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configStaticBinName.json", "src/test/resources/dataStaticBin.csv"});
		});


		boolean error = getError(log);

		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");

		System.out.println("Test static BinName: Complete");
	}

	//Validate map sort order
	@Test
	public void testValidateMapOrder() throws Exception {
		System.out.println("TestValidateMapOrder: start");
		if(!client.isConnected()) {
			System.out.println("Client is not able to connect:" + host + ":" + port);
			return;
		}
		
		// Create datafile

		HashMap<String, String> binMap = (HashMap<String, String>) testSchema.get("test_map");

		
		int setMod = 5, range = 100, seed = 10, nrecords = 10;
		dataFile = rootDir + "dataMap.dsv";
		writeDataMap(dataFile, nrecords, setMod, range, seed, binMap);
		
		// Run Aerospike loader
		this.expectedMapOrder = MapOrder.UNORDERED;
		int exitCode = SystemLambda.catchSystemExit(() -> {
			AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action, "-um", "-c", "src/test/resources/configMap.json", dataFile});
		});

		// Validate loaded data
		String dstType = "map";
		boolean dataValid = validateMap(client, dataFile, nrecords, setMod, range, seed, binMap, dstType);
		boolean error = getError(log);

		assertTrue(dataValid);
		assertTrue(!error);
		assertEquals(0, exitCode, "Unexpected exit code");
		this.expectedMapOrder = MapOrder.KEY_ORDERED;

		System.out.println("TestValidateMap: Complete");
	}

	// Helper functions
	public void writeDataMap(String fileName, int nrecords, int setMod, int range, int seed,
			HashMap<String, String> binMap) {
		String delimiter = (String) testSchema.get("delimiter");
		File file = new File(fileName);
		// if file doesnt exists, then create it

		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			Writer bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF8"));
			Random r = new Random(seed);
			int rint;
			for (int i = 0; i <= nrecords; i++) {
				int comma = 0;

				rint = r.nextInt(range);

				Iterator<Entry<String, String>> iterator = binMap.entrySet().iterator();
				String binName;
				String binType;
				Map.Entry<String, String> mapEntry;
				while (iterator.hasNext()) {
					mapEntry = iterator.next();
					binName = mapEntry.getKey();
					binType = mapEntry.getValue();
					if (binName != null) {
						if (i == 0) {
							bw.write(binName);
						} else {

							if (binName.equalsIgnoreCase("key")) {
								bw.write(getValue(binName, binType, i));
							} else if (binName.equalsIgnoreCase("set")) {
								bw.write(getValue(binName, "string", i % setMod));
							} else {
								//bw.write(getValue(binName, binType, rint));
								// TODO this int shouln't be random.
								bw.write(getValue(binName, binType, i));
							}
						}
					}
					if (binMap.size() > ++comma) {
						bw.write(String.format(delimiter));
					}
				}
				bw.write("\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean validateMap(AerospikeClient client, String filename, int nrecords, int setMod, int range, int seed,
			HashMap<String, String> binMap, String dstType) {
		boolean valid = false;
		Random r = new Random(seed);
		int rint;
		String as_binname_suffix = (String) testSchema.get("as_binname_suffix");

		for (int i = 1; i <= nrecords; i++) {

			String key = null;
			String set = null;
			Key key1 = null;
			Bin bin1 = null;
			String bin1Type = null;
			Record record = null;
			
			rint = r.nextInt(range);

			Iterator<Entry<String, String>> iterator = binMap.entrySet().iterator();
			String binName;
			String binType = null;
			Map.Entry<String, String> mapEntry;
			while (iterator.hasNext()) {
				mapEntry = iterator.next();
				if ((binName = mapEntry.getKey()) == null) {
					continue;
				}
				/*
				if (i == 0) {
					// skip 1st row data
					continue;
				}
				*/
				binType = mapEntry.getValue();

				if (binName.equalsIgnoreCase("key")) {
					key = (String.format(getValue(binName, binType, i)));
				} else if (binName.equalsIgnoreCase("set")) {
					set = String.format(getValue(binName, "string", i % setMod));
				} else {
					// TODO this int shouln't be random.
					//String value = (String.format(getValue(binName, binType, rint)));
					String value = (String.format(getValue(binName, binType, i)));

					// We are writing Binname in aerospike as (Column name +
					// as_binname_suffix)
					// Just to make more flexible naming.
					binName = binName + as_binname_suffix;
					bin1 = new Bin(binName, value);
					// bin1Type = value;
					bin1Type = binType;
				}
			}
			/*
			if (i != 0) {
				continue;
			}
			*/
			try {
				key1 = new Key(ns, set, key);
				record = client.get(new Policy(), key1);
			} catch (AerospikeException e) {
				e.printStackTrace();
			}

			if (validateBin(key1, bin1, bin1Type, dstType, record))
				valid = true;
		}

		return valid;
	}

	/**
	 * @param key key to validate
	 * @param bin bin to validate
	 * @param binType type of bin
	 * @param dstType dst bin type
	 * @param record received record
	 * @return
	 */
	private boolean validateBin(Key key, Bin bin, String binType, String dstType, Record record) {
		boolean valid = false;
		String expected = null;

		Object received = record.bins.get(bin.name);
		if (binType != null && binType.equalsIgnoreCase("timestamp")
				&& dstType != null && dstType.equalsIgnoreCase("integer")) {

			DateFormat format = new SimpleDateFormat("MM/dd/yy");
			try {
				Date formatDate = format.parse(bin.value.toString());
				long miliSecondForDate = formatDate.getTime() / 1000;
				expected = String.format("%d", miliSecondForDate);
			} catch (java.text.ParseException e) {
				e.printStackTrace();
			}
			
		} else if (dstType != null && dstType.equalsIgnoreCase("blob")) {
			expected = convertHexToString(bin.value.toString());
			received = new String((byte[]) received);
		} else if (dstType != null && (dstType.equalsIgnoreCase("list"))) {
			received = received.toString().replace("=", ":");
			expected = bin.value.toString().replace("'", "");
			expected = expected.replace("\"", "");

		} else if (dstType != null && (dstType.equalsIgnoreCase("json"))) {
			System.out.println(String.format("Currently json can not be matched."));

			//received = received.toString().replace("=", ":");
			//expected = bin.value.toString().replace("'", "");
			//expected = expected.replace("\"", "");

			return true;
		} else if (dstType != null && (dstType.equalsIgnoreCase("map"))) {
			System.out.println(String.format("Currently only map order is checked."));

			//received = received.toString().replace("=", ":");
			//expected = bin.value.toString().replace("'", "");
			//expected = expected.replace("\"", "");

			MapOrder mapOrder = (received instanceof SortedMap<?,?>)? MapOrder.KEY_ORDERED : MapOrder.UNORDERED;

			if (mapOrder == this.expectedMapOrder) {
				return true;
			}

			return false;
		} else {
			expected = bin.value.toString();
		}
		
		if (received != null && received.toString().equals(expected)) {
			System.out.println(String.format(
					"Bin matched: namespace=%s set=%s key=%s bin=%s value=%s generation=%d expiration=%d",
					key.namespace, key.setName, key.userKey, bin.name, received, record.generation, record.expiration));
			valid = true;
		} else {
			System.out.println(String.format("Put/Get mismatch: Expected %s. Received %s.", expected, received));
		}


		return valid;
	}

	
	/**
	 * @param log log file name
	 * @return return true if get any error
	 */
	public boolean getError(String log) {
		boolean error = false;

		BufferedReader br = null;
		try {
			String line;
			br = new BufferedReader(new FileReader(log));
			try {
				while ((line = br.readLine()) != null) {
					if ((line.substring(0, 5)).contains("ERROR")) {
						error = true;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return error;
	}
	
	/**
	 * @param binName binName prefix
	 * @param binType type of binValue
	 * @param i index of insert
	 * @return
	 */
	public String getValue(String binName, String binType, int i) {

		String value = null;

		switch (getBinType(binType.toLowerCase())) {
		case BLOB:
			value = convertStringToHex(String.format("%s%d", binName, i));
			break;
		case INTEGER:
			value = String.format("%d", i);
			break;
		case JSON:
			JSONParser jsonParser = new JSONParser();
			value = "{\"k1\": \"v1\", \"k2\": [\"lv1\", \"lv2\"], \"k3\": {\"mk1\": \"mv1\"}}";
			break;
		case LIST:
			value = "[\"a\", \"b\", \"c\", [\"d\", \"e\"]]";
			break;
		case MAP:
			value = "{\"a\":\"b\", \"c\":{\"e\":\"d\"}, \"b\":\"c\"}";
			// sorted value should be "{\"a\":\"b\", \"b\":\"c\", \"c\":{\"d\":\"e\"}}";
			break;
		case STRING:
			if (binName.equalsIgnoreCase("utf8")) {
				value = String.format("%s%d", "Ûtf8", i);
			} else
				value = String.format("%s%d", binName, i);
			break;
		case TIMESTAMP:
			value = String.format("%d/%d/%d", i % 12, i % 30, i % 100);
			break;
		default:
			break;
		}

		return value;
	}

	public BinType getBinType(String type) {
		if ("string".equalsIgnoreCase(type)) {
			return BinType.STRING;
		} else if ("integer".equalsIgnoreCase(type)) {
			return BinType.INTEGER;
		} else if ("blob".equalsIgnoreCase(type)) {
			return BinType.BLOB;
		} else if ("list".equalsIgnoreCase(type)) {
			return BinType.LIST;
		} else if ("map".equalsIgnoreCase(type)) {
			return BinType.MAP;
		} else if ("json".equalsIgnoreCase(type)) {
			return BinType.JSON;
		} else if ("timestamp".equalsIgnoreCase(type)) {
			return BinType.TIMESTAMP;
		}
		return null;
	}

	public String convertStringToHex(String str) {

		char[] chars = str.toCharArray();
		StringBuffer hex = new StringBuffer();
		for (int i = 0; i < chars.length; i++) {
			hex.append(Integer.toHexString((int) chars[i]));
		}
		return hex.toString();
	}

	public String convertHexToString(String hex) {

		StringBuilder sb = new StringBuilder();
		StringBuilder temp = new StringBuilder();

		for (int i = 0; i < hex.length() - 1; i += 2) {
			// get the hex in pairs
			String pair = hex.substring(i, (i + 2));
			// convert hex to decimal
			int numpair = Integer.parseInt(pair, 16);
			// convert the decimal to character
			sb.append((char) numpair);

			temp.append(numpair);
		}
		return sb.toString();
	}
}
