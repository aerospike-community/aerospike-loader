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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.admin.Role;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

/**
 * This is the main class for the Aerospike import. 
 * 
 * It will import multiple Data Dump files concurrently
 * 
 * To run: java -jar aerospike-import-<version> <options> <file names> *
 * @author Aerospike
 *
 */
public class AerospikeLoad implements Runnable {

	private AerospikeClient 	client;
	private String				fileName;
	
	// Config related variable
	private static ExecutorService	writerPool;
	private static int				nWriterThreads;
	private static int				nReaderThreads;
	private static int 				maxConnsPerNode;
	
	private static final int		scaleFactor = 5;
	private static String 			DEFAULT_DELIMITER = ",";
	private static String 			DEFAULT_HEADER_EXIST = "false";
	
	// Other variables.
	private static Parameters 		params;
	private static Counter 			counters;
	private static Thread 			statPrinter;
	
	// Data definition related variable
	private static HashMap<String, String>	dsvConfigs;
	private static List<MappingDefinition>	mappingDefs;

	private static Logger log = LogManager.getLogger(AerospikeLoad.class);


	private static void printVersion()
	{
		final Properties properties = new Properties();
		try {
			properties.load(AerospikeLoad.class.getClassLoader().getResourceAsStream("project.properties"));
		} catch (Exception e) {
			System.out.println("None");
		} finally {
			System.out.println(properties.getProperty("name"));
			System.out.println("Version " + properties.getProperty("version"));
		}
	}

        public static void main(String[] args) throws IOException {
	    int exitCode = -1;
	    try {
	        CommandLine cl = parseArgs(args);
	        exitCode = launch(cl);
	    } catch (Exception e) {
	        if (log.isDebugEnabled()) {
		    e.printStackTrace();
	        }
	    }
	    System.exit(exitCode);
        }

        public static int launch(CommandLine cl) throws Exception {
	    long processStart = System.currentTimeMillis();

	    AerospikeClient client = null;
	    statPrinter = new Thread(new PrintStat(counters));
	    // Create Abstract derived params from provided commandline params.
	    params = Utils.parseParameters(cl);
	    if (params.verbose) {
	        Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
	    }

	    initReadWriteThreadCnt(cl);

	    // Get and validate user roles for client.
	    client = getAerospikeClient(cl);
	    if (client == null) {
	        return -1;
	    }

	    List<String> dataFileNames = new ArrayList<String>();
	    initDataFileNameList(cl, dataFileNames);
	    if (dataFileNames.size() == 0) {
	        return -1;
	    }

	    // Remove column definition file from list. if directory containing config file is passed.
	    String columnDefinitionFileName = cl.getOptionValue("c", "");
	    dataFileNames.remove(columnDefinitionFileName);
	    log.info("Number of data files:" + dataFileNames.size());

	    initBytesToRead(dataFileNames);

	    log.info("Aerospike loader started");
	    // Perform main Read Write job.
	    runLoader(client, columnDefinitionFileName, dataFileNames);

	    // Stop statistic printer thread.
	    statPrinter.interrupt();
	    log.info("Aerospike loader completed");
	    if (client != null) {
	        client.close();
	    }

	    long processStop = System.currentTimeMillis();
	    log.info(String.format("Loader completed in %.3fsec", (float) (processStop - processStart) / 1000));
	    return 0;
        }

        public static CommandLine parseArgs(String[] args) throws ParseException {
	    counters = new Counter();
	    CommandLine cl;

	    Options options = new Options();
	    options.addOption("h", "hosts", true,
			      "List of seed hosts in format:\n" +
			      "hostname1[:tlsname][:port1],...\n" +
			      "The tlsname is only used when connecting with a secure TLS enabled server. " +
			      "If the port is not specified, the default port is used.\n" +
			      "IPv6 addresses must be enclosed in square brackets.\n" +
			      "Default: localhost\n" +
			      "Examples:\n" +
			      "host1\n" +
			      "host1:3000,host2:3000\n" +
			      "192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n"
			      );
	    options.addOption("V", "version", false, "Aerospike Loader Version");
	    options.addOption("p", "port", true, "Server port (default: 3000)");
	    options.addOption("U", "user", true, "User name");
	    options.addOption("P", "password", true, "Password");
	    options.addOption("n", "namespace", true, "Namespace (default: test)");
	    options.addOption("c", "config", true, "Column definition file name");
	    options.addOption("g", "max-throughput", true, "It limit numer of writes/sec in aerospike.");
	    options.addOption("T", "transaction-timeout", true, "write transaction timeout in milliseconds(default: No timeout)");
	    options.addOption("e", "expirationTime", true,
			      "Set expiration time of each record in seconds." +
			      " -1: Never expire, " +
			      "  0: Default to namespace," +
			      " >0: Actual given expiration time"
			      );
	    options.addOption("tz", "timezone", true, "Timezone of source where data dump is taken (default: local timezone)");
	    options.addOption("ec", "abort-error-count", true, "Error count to abort (default: 0)");
	    options.addOption("wa", "write-action", true, "Write action if key already exists (default: update)");
	    options.addOption("sa", "services_alternate", false, "Enable alternate services.");
	    options.addOption("tls", "tls-enable", false, "Use TLS/SSL sockets");
	    options.addOption("tp", "tls-protocols", true,
			      "Allow TLS protocols\n" +
			      "Values:  TLSv1,TLSv1.1,TLSv1.2 separated by comma\n" +
			      "Default: TLSv1.2"
			      );
	    options.addOption("tlsCiphers", "tls-cipher-suite", true,
			      "Allow TLS cipher suites\n" +
			      "Values:  cipher names defined by JVM separated by comma\n" +
			      "Default: null (default cipher list provided by JVM)"
			      );
	    options.addOption("tr", "tlsRevoke", true,
			      "Revoke certificates identified by their serial number\n" +
			      "Values:  serial numbers separated by comma\n" +
			      "Default: null (Do not revoke certificates)"
			      );

	    options.addOption("tlsLoginOnly", false, "Use TLS/SSL sockets on node login only");
	    options.addOption("auth", true, "Authentication mode. Values: " + Arrays.toString(AuthMode.values()));

	    options.addOption("uk", "send-user-key", false,
			      "Send user defined key in addition to hash digest to store on the server. (default: userKey is not sent to reduce meta-data overhead)"
			      );
	    options.addOption("v", "verbose", false, "Logging all");
	    options.addOption("um", "unorderdMaps", false, "Write all maps as unorderd maps");
	    options.addOption("u", "usage", false, "Print usage.");

	    CommandLineParser parser = new PosixParser();
	    cl = parser.parse(options, args, false);

	    if (args.length == 0 || cl.hasOption("u")) {
	        printUsage(options);
	        return cl;
	    }

	    if (cl.hasOption("V")) {
	        printVersion();
	        return cl;
	    }

	    return cl;
        }

	private static AerospikeClient getAerospikeClient(CommandLine cl) {
		ClientPolicy clientPolicy = new ClientPolicy();	
		
		initClientPolicy(cl, clientPolicy);
		
		AerospikeClient client = new AerospikeClient(clientPolicy, params.hosts);

		if (!client.isConnected()) {
			log.error("Client is not able to connect:" + params.hosts);
			return null;
		}
		return client;
	}

	private static void initClientPolicy(CommandLine cl, ClientPolicy clientPolicy) {

		if (cl.hasOption("auth")) {
			clientPolicy.authMode = AuthMode.valueOf(cl.getOptionValue("auth", "").toUpperCase());
		}				
		// Setting user, password in client policy.
		clientPolicy.user = cl.getOptionValue("user");
		clientPolicy.password = cl.getOptionValue("password");

		if (clientPolicy.user != null && clientPolicy.password == null) {
			java.io.Console console = System.console();
			try {
				if (console != null) {
					char[] pass = console.readPassword("Enter password:");

					if (pass != null) {
						clientPolicy.password = new String(pass);
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		// Setting TLS in ClinetPolicy.
		if (cl.hasOption("tls")) {
			clientPolicy.tlsPolicy = new TlsPolicy();

			if (cl.hasOption("tp")) {
				String s = cl.getOptionValue("tp", "");
				clientPolicy.tlsPolicy.protocols = s.split(",");
			}

			if (cl.hasOption("tlsCiphers")) {
				String s = cl.getOptionValue("tlsCiphers", "");
				clientPolicy.tlsPolicy.ciphers = s.split(",");
			}

			if (cl.hasOption("tr")) {
				String s = cl.getOptionValue("tr", "");
				clientPolicy.tlsPolicy.revokeCertificates = Util.toBigIntegerArray(s);
			}
		}

		if (cl.hasOption("sa")) {
			clientPolicy.setUseServicesAlternate(true);
		}

		clientPolicy.maxConnsPerNode = maxConnsPerNode;
	}
	
	private static void initReadWriteThreadCnt(CommandLine cl) {
		// Get available processors to calculate default number of threads.
		int cpus = Runtime.getRuntime().availableProcessors();
		nWriterThreads = cpus * scaleFactor;
		nReaderThreads = cpus;

		nWriterThreads = (nWriterThreads > 0
				? (nWriterThreads > Constants.MAX_THREADS ? Constants.MAX_THREADS : nWriterThreads) : 1);
		log.debug("Using writer Threads: " + nWriterThreads);

		nReaderThreads = (nReaderThreads > 0
				? (nReaderThreads > Constants.MAX_THREADS ? Constants.MAX_THREADS : nReaderThreads) : 1);
		log.debug("Using reader Threads: " + nReaderThreads);

		// add 1 for the tend thread
		maxConnsPerNode = nWriterThreads + nReaderThreads + 1;
		log.debug("Max connections per node: " + maxConnsPerNode);
	}
	
	private static void initDataFileNameList(CommandLine cl, List<String> dataFileNames) {
		// Get data file list.
		String[] dataFiles = cl.getArgs();
		
		// Get data filename list.
		if (dataFiles.length == 0) {
			log.error("No data file Specified: add <file/dir name> to end of the command");
			return;
		}
		
		dataFileNames.addAll(Utils.getFileNames(dataFiles));
		
		if (dataFileNames.size() == 0) {
			log.error("Given datafiles/directory does not exist Files: " + dataFileNames.toString());
			return;
		}
	}

	private static void initBytesToRead(List<String> dataFileNames) {
		for (int i = 0; i < dataFileNames.size(); i++) {
			log.debug("File names:" + Utils.getFileName(dataFileNames.get(i)));
			File file = new File(dataFileNames.get(i));
			counters.write.bytesTotal = counters.write.bytesTotal + file.length();
		}
	}

	private static void runLoader(AerospikeClient client, String columnDefinitionFileName, List<String> dataFileNames) throws Exception {
		/*
		 * Process column definition file to get,
		 * dsv_configs (delimiter, header_exist, n_column)
		 * mapping definition(key, set, binlist)
		 */
		File columnDefinitionFile = getColumnDefinitionFile(columnDefinitionFileName);
		
		dsvConfigs = new HashMap<String, String>();
		mappingDefs = new ArrayList<MappingDefinition>();
		
		if (Parser.parseJSONColumnDefinitions(columnDefinitionFile, dsvConfigs, mappingDefs)) {
			log.info("Config file processed.");
		} else {
			throw new Exception("Config file parsing Error");
		}

		// Validate and set default DsvConfigs (from config/definition file).
		validateAndSetDefaultDsvConfig();

		// Parse datafile header line to get column names.
		List <String> columnNames = parseColumnNames(columnDefinitionFileName, dataFileNames);

		updateColumnInfoForMappingDefs(columnNames);

		validateMappingDefs();

		statPrinter.start();
		
		// writerPool is global as it will be used outside.
		writerPool = Executors.newFixedThreadPool(nWriterThreads);

		ExecutorService readerPool = Executors.newFixedThreadPool(nReaderThreads > dataFileNames.size()
										? dataFileNames.size() : nReaderThreads);
		log.info("Reader pool size : " + nReaderThreads);
		
		// Read write process start from this point.
		counters.write.writeStartTime = System.currentTimeMillis();

		// Submit all tasks to reader thread pool.
		for (String aFile : dataFileNames) {
			log.debug("Submitting task for: " + aFile);
			readerPool.submit(new AerospikeLoad(aFile, client));
		}
		
		// Wait for reader pool to complete.
		readerPool.shutdown();
		log.info("Shutdown reader thread pool");
		
		while(!readerPool.isTerminated());
		log.info("Reader thread pool terminated");
		
		// Wait for writer pool to complete after getting all tasks from reader pool.
		writerPool.shutdown();
		log.info("Shutdown writer thread pool");
		
		while(!writerPool.isTerminated());
		log.info("Writer thread pool terminated");

		// Print final statistic of aerospike-loader.
		log.info("Final Statistics of importer: ("
				+ "Records Read = " + counters.write.readCount.get() + ", "
				+ "Successful Writes = " + counters.write.writeCount.get() + ", "
				+ "Successful Primary Writes = " + (counters.write.writeCount.get() - counters.write.mappingWriteCount.get()) + ", "
				+ "Successful Mapping Writes = " + counters.write.mappingWriteCount.get() + ", "
				+ "Errors = " + (counters.write.writeErrors.get() + counters.write.readErrors.get() + counters.write.processingErrors.get())
				+ "(" + (counters.write.writeErrors.get()) + "-Write," + counters.write.readErrors.get() + "-Read,"
				+ counters.write.processingErrors.get() + "-Processing), " + "Skipped = "
				+ (counters.write.keyNullSkipped.get() + counters.write.noBinsSkipped.get()) + "("
				+ (counters.write.keyNullSkipped.get()) + "-NullKey," + counters.write.noBinsSkipped.get()
				+ "-NoBins)");
	}

	private static File getColumnDefinitionFile(String columnDefinitionFileName) throws Exception {
		log.debug("Column definition files/directory: " + columnDefinitionFileName);

		if (columnDefinitionFileName == null) {
			throw new Exception("Column definition files/directory not specified. use -c <file name>");
		}

		File columnDefinitionFile = new File(columnDefinitionFileName);
		if (!columnDefinitionFile.exists()) {
			throw new Exception("Column definition files/directory does not exist: "
					+ Utils.getFileName(columnDefinitionFileName));
		}
		return columnDefinitionFile;
	}

	private static void validateAndSetDefaultDsvConfig() throws Exception {
		// Version check.
		String version = dsvConfigs.get(Constants.VERSION);
		if (!isVersionSupported(version)) {
			throw new Exception("\"" + Constants.VERSION + ":" + version + "\" is not Supported");
		}

		// Set default delimiter.
		String delimiter = dsvConfigs.get(Constants.DELIMITER);
		if (delimiter == null) {
			log.warn("\"" + Constants.DELIMITER
					+ "\" is not properly specified in config file. Default is ','");
			dsvConfigs.put(Constants.DELIMITER, DEFAULT_DELIMITER);
		}

		// Set Default header exist.
		String isHeaderExist = dsvConfigs.get(Constants.HEADER_EXIST);
		if (isHeaderExist == null) {
			log.warn("\"" + Constants.HEADER_EXIST
					+ "\" is not properly specified in config file. Default is false");
			dsvConfigs.put(Constants.HEADER_EXIST, DEFAULT_HEADER_EXIST);
		}
	}

	private static boolean isVersionSupported(String version) {
		// Version check
		String[] vNumber = version.split("\\.");
		int v1 = Integer.parseInt(vNumber[0]);
		int v2 = Integer.parseInt(vNumber[1]);
		if ((v1 <= Constants.MajorV) && (v2 <= Constants.MinorV)) {
			log.debug("Config version used:" + version);
			return true;
		} else {
			return false;
		}
	}

	private static List<String> parseColumnNames(String columnDefinitionFileName, List<String> dataFileNames) throws Exception {

		// Parse header line from data file to get Bin names.
		List<String> columnNames = null;

		if (dsvHasHeader()) {
			String dataFileName = dataFileNames.get(0);
			String line;
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFileName), "UTF8"));
			if ((line = br.readLine()) != null) {
				columnNames = Parser.getDSVRawColumns(line, dsvConfigs.get(Constants.DELIMITER));
				br.close();
			}
			else{
				br.close();
				throw new Exception("Header line not found." + " Datafile: " + Utils.getFileName(dataFileName));
			}
		}

		// Throw exception if n_column defined in columndef file doesn't match n_columns in datafile. 
		if (columnNames != null && (columnNames.size() != Integer.parseInt(dsvConfigs.get(Constants.N_COLUMN)))) {
			throw new Exception("Number of column in config file and datafile are mismatch." + " Datafile: "
					+ Utils.getFileName(dataFileNames.get(0)) + " columns: " + columnNames.toString() + " Configfile: "
					+ Utils.getFileName(columnDefinitionFileName) + " n_colums: " + Integer.parseInt(dsvConfigs.get(Constants.N_COLUMN)));
		}
		return columnNames;
	}

	private static void updateColumnInfo(ColumnDefinition columnDef, List<String> columnNames) throws Exception {

		if (columnDef.columnPos < 0) {
			if (columnDef.columnName == null) {
				if (columnDef.jsonPath == null) {
					throw new Exception("Improper column definition. Please mention column pos / column name or json path.");
				} else {
					throw new Exception("JSON path is not a supported feature.");
				}
			} else if (dsvHasHeader()) {
				columnDef.columnPos = columnNames.indexOf(columnDef.columnName);

				if (columnDef.columnPos == -1) {
					throw new Exception("Missing column name " + columnDef.columnName + " in data file header.");
				}
			}
		} 

		if (dsvHasHeader()) {
			columnDef.columnName = columnNames.get(columnDef.columnPos);
		}
	}

	private static void updateColumnInfoForMappingDefs(List<String> columnNames) throws Exception {

		for (MappingDefinition mappingDef : mappingDefs) {

			// KEY
			if (mappingDef.keyColumnDef == null) {
				throw new Exception ("Mapping definition without key mapping");	
			}
			updateColumnInfo(mappingDef.keyColumnDef.nameDef, columnNames);

			// SET
			if (mappingDef.setColumnDef.staticName == null) {
				if (mappingDef.setColumnDef.nameDef != null) {
					updateColumnInfo(mappingDef.setColumnDef.nameDef, columnNames);
				} else {
					throw new Exception("Set Not defined in mapping definition. null set not allowed !!");
				}
			}

			// BINS
			for (BinDefinition binColumnDef : mappingDef.binColumnDefs) {

				// BIN NAME
				if (binColumnDef.staticName == null) {
					if (binColumnDef.nameDef != null) {
						updateColumnInfo(binColumnDef.nameDef, columnNames);
					} else {
						throw new Exception("Mapping Definition with missing bin Name definition");
					}
				}

				// BIN VALUE
				if (binColumnDef.staticValue == null) {
					if (binColumnDef.valueDef != null) {

						// SYSTEM_TIME is reserved column value
						if (binColumnDef.valueDef.columnName != null && binColumnDef.valueDef.columnName.toLowerCase().equals(Constants.SYSTEM_TIME)) {
							continue;
						}
						updateColumnInfo(binColumnDef.valueDef, columnNames);
					} else {
						throw new Exception("Mapping Definition with missing bin value definition");
					}
				}
			}
			log.debug("MappingDef:" + mappingDef.toString());
		}
	}

	private static void validateMappingDefs() throws Exception {

		for (MappingDefinition mappingDef : mappingDefs) {
			validateSetNameInfo(mappingDef.setColumnDef);

			validateKeyNameInfo(mappingDef.keyColumnDef);

			for (BinDefinition binColumnDef : mappingDef.binColumnDefs) {

				validateBinColumnsNameInfo(binColumnDef);
				validateBinColumnsValueInfo(binColumnDef);

				// TODO - arguably not needed. Pretty arbitrary
				if ((binColumnDef.nameDef.columnPos == binColumnDef.valueDef.columnPos)
						&& (binColumnDef.nameDef.columnPos != -1)) {
					throw new Exception("Dynamic Bin Name column info same as Dynamic bin Value" + binColumnDef);
				}
			}
		}
	}

	private static void validateSetNameInfo(MetaDefinition metadataColDef) throws Exception {
		
		if ((metadataColDef.staticName != null && metadataColDef.staticName.length() > Constants.SET_NAME_LENGTH)) {
			throw new Exception("Set name len exceed Allowd limit. SET_NAME_LEN_MAX: " + Constants.SET_NAME_LENGTH + "Given SetName: " + metadataColDef.staticName);
		}
		
		if (metadataColDef.staticName != null) {
			return;
		}

		if (metadataColDef.nameDef.columnPos < 0 && (metadataColDef.nameDef.columnName == null)) {
			throw new Exception("Information missing(columnName, columnPos) in config file: " + metadataColDef);
		}

		if (metadataColDef.nameDef.srcType == null
				|| !metadataColDef.nameDef.srcType.equals(SrcColumnType.STRING)) {
			throw new Exception("Set name should be string type: " + metadataColDef);
		}

	}

	private static void validateKeyNameInfo(MetaDefinition metadataColDef) throws Exception {
		
		if (metadataColDef.nameDef.columnPos < 0 && (metadataColDef.nameDef.columnName == null)) {
			throw new Exception("Information missing(columnName, columnPos) in config file: " + metadataColDef);
		}

		if (metadataColDef.nameDef.srcType == null) {
			throw new Exception("Source data type is not properly mentioned: " + metadataColDef);
		}
	}

	private static void validateBinColumnsNameInfo(BinDefinition binColumnDef) throws Exception {
		if ((binColumnDef.staticName != null && binColumnDef.staticName.length() > Constants.BIN_NAME_LENGTH)) {
			throw new Exception("Bin name len exceed Allowd limit. BIN_NAME_LEN_MAX: " + Constants.BIN_NAME_LENGTH + "Given BinName: " + binColumnDef.staticName);
		}
		
		if (binColumnDef.staticName != null) {
			return;
		}

		if ((binColumnDef.nameDef.columnPos < 0) && binColumnDef.nameDef.columnName == null) {
			throw new Exception("Information missing(columnName, columnPos) in config file: " + binColumnDef);
		}
	}

	private static void validateBinColumnsValueInfo(BinDefinition binColumnDef) throws Exception {

		if (binColumnDef.staticValue != null) {
			return;
		}

		// check for missing entries in config file
		if (binColumnDef.valueDef.columnPos < 0 && binColumnDef.valueDef.columnName == null) {
			throw new Exception(
					"Information missing(columnName, columnPos) in config file: " + binColumnDef);
		}

		// src_type is mandatory.
		if (binColumnDef.valueDef.srcType == null) {
			throw new Exception("Source data type is not properly mentioned:" + binColumnDef);
		}

		// TIMESTAMP, BLOB src_type needs a dst_type.
		if ((binColumnDef.valueDef.srcType.equals(SrcColumnType.TIMESTAMP)
					|| binColumnDef.valueDef.srcType.equals(SrcColumnType.BLOB))
				&& binColumnDef.valueDef.dstType == null) {
			throw new Exception("Destination type is not mentioned: " + binColumnDef);
		}

		// Encoding should be given if dst_type is given(not needed in dst_type CDT_LIST)
		if (binColumnDef.valueDef.dstType != null
				&& binColumnDef.valueDef.encoding == null) {
			throw new Exception("Encoding is not given for src-dst type conversion:" + binColumnDef);
		}

		// BLOB dst_type needs HEX_ENCODING.
		if (binColumnDef.valueDef.srcType.equals(SrcColumnType.BLOB)
				&& !binColumnDef.valueDef.encoding.equals(Constants.HEX_ENCODING)) {
			throw new Exception("Wrong encoding for blob data:" + binColumnDef);
		}
	}
	
	/*
	 * Write help/usage to console.
	 */
	private static void printUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = AerospikeLoad.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		log.info(sw.toString());
	}

	private static boolean dsvHasHeader() {
		return Boolean.valueOf(dsvConfigs.get(Constants.HEADER_EXIST));
	}

	/*
	 * Constructor
	 */
	public AerospikeLoad(String fileName, AerospikeClient client) throws AerospikeException {
		this.client = client;
		this.fileName = fileName;
	}

	/*
	 * Process a single file
	 */
	private void processFile() {
		int lineNumber = 0;

		log.trace("Hosts: " + this.client.getNodeNames());
		long start = System.currentTimeMillis();

		try {

			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(this.fileName), "UTF8"));
			log.debug("Reading file:  " + Utils.getFileName(fileName));
			boolean hasHeader = dsvHasHeader();

			while (br.ready()) {
				String line;

				// Skip reading 1st line of data file
				if (hasHeader) {
					lineNumber++;
					String header = br.readLine();
					counters.write.bytesTotal = counters.write.bytesTotal - header.length();
				}

				while ((line = br.readLine()) != null) {
					lineNumber++;
					log.trace("Read line " + lineNumber + " from file: " + Utils.getFileName(fileName));

					// Throttle if read to write difference goes beyond given number.
					while ((counters.write.processingQueued.get()
							- counters.write.processingCount.get()) > Constants.RW_THROTTLE_LIMIT) {
						Thread.sleep(20);
					}

					List<String> columns = Parser.getDSVRawColumns(line, dsvConfigs.get(Constants.DELIMITER));

					if (columns == null) {
						continue;
					}

					for (MappingDefinition mappingDef : mappingDefs) {
						counters.write.processingQueued.getAndIncrement();
						writerPool.submit(new AsWriterTask(fileName, lineNumber, (line.length() + 1),  this.client, columns,
								dsvConfigs, mappingDef, params, counters));
					}
					
					log.trace("Submitting line " + lineNumber + " in file: " + Utils.getFileName(fileName));
					counters.write.readCount.incrementAndGet();
				}
			}

			br.close();

		} catch (Exception e) {
			log.error("Error processing file: " + Utils.getFileName(fileName) + ": Line: " + lineNumber);
			if (log.isDebugEnabled()) {
				e.printStackTrace();
			}
			counters.write.readErrors.getAndIncrement();
		}

		long stop = System.currentTimeMillis();
		log.info(String.format("Reader completed %d-lines in %.3fsec, From file: %s", lineNumber,
				(float) (stop - start) / 1000, Utils.getFileName(fileName)));
	}
	
	public void run() {
		log.info("Processing: " + Utils.getFileName(fileName));

		try {
			log.trace("Processing File: " + Utils.getFileName(fileName));
			processFile();
		} catch (Exception e) {
			log.error("Cannot process file: " + Utils.getFileName(fileName));
			if (log.isDebugEnabled()) {
				e.printStackTrace();
			}
		}
	}
}
