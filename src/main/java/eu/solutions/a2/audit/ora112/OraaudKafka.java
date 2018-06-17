/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package eu.solutions.a2.audit.ora112;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.audit.BaseAgentLauncher;
import eu.solutions.a2.audit.DaemonThreadFactory;
import eu.solutions.a2.audit.ora112.schema.Audit;
import eu.solutions.a2.audit.utils.ExceptionUtils;
import eu.solutions.a2.audit.utils.file.OpenFileGenericNio;
import eu.solutions.a2.audit.utils.file.OpenFileGenericX;
import eu.solutions.a2.audit.utils.file.OpenFileSystemV;
import eu.solutions.a2.audit.utils.file.OpenFilesIntf;

import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;

public class OraaudKafka extends BaseAgentLauncher {

	/**   Default interval of locked file queue refresh */
	private static final int LOCKED_QUEUE_REFRESH_INTERVAL = 1000;

	private static final Logger LOGGER = LoggerFactory.getLogger(OraaudKafka.class);

	private static ConcurrentLinkedQueue<String> lockedFilesQueue = null;
	private static OpenFilesIntf fileLockChecker = null;

	/**   Oracle audit xml unmarshaller */
	private static AuditFileJAXBUnmarshallerSingleton unmarshaller = null;
	/**   Main thread pool for Kafka jobs */
	private static ThreadPoolExecutor threadPool;
	/**  Kafka producer, we use object due to different modes of work */
	private static Producer<String, Object> producer;
	/** hostname **/
	private static String hostName;
	/** Windows OS? **/
	private static boolean isWinOs = false;

	private static final String SEND_FILE_KAFKA_LIGHT = "kafka-light";
	private static final String SEND_FILE_KAFKA_FULL = "kafka-full";
	private static final String SEND_FILE_CASSANDRA = "cassandra";
	protected static final int SEND_FILE_KAFKA_LIGHT_INT = 0;
	protected static final int SEND_FILE_KAFKA_FULL_INT = 1;
	protected static final int SEND_FILE_CASSANDRA_INT = 2;
	protected static int sendFileMethod = SEND_FILE_KAFKA_LIGHT_INT;

	//TODO
	//TODO - add MB https://www.ibm.com/developerworks/ru/library/j-jtp09196/index.html
	//TODO
	public static void watchDirectoryPath(Path path) {

		String watchedDir = path.toAbsolutePath().toString() + File.separator;
		LOGGER.info("Watching path: " + path.toString());

		// We obtain the file system of the Path
		FileSystem fs = path.getFileSystem();

		try (WatchService service = fs.newWatchService()) {
			// We're interrested only in new files!
			path.register(service, StandardWatchEventKinds.ENTRY_CREATE);

			// Start the infinite polling loop
			WatchKey key = null;
			while (true) {
				key = service.take();

				// Dequeueing events
				Kind<?> kind = null;
				for (WatchEvent<?> watchEvent : key.pollEvents()) {
					// Get the type of the event
					kind = watchEvent.kind();
					 if (StandardWatchEventKinds.OVERFLOW == kind) {
						 continue; // loop
					 } else if (StandardWatchEventKinds.ENTRY_CREATE == kind) {
						// A new Path was created
						@SuppressWarnings("unchecked")
						Path newPath = ((WatchEvent<Path>) watchEvent).context();
						String fileName = watchedDir + newPath.getFileName(); 
						LOGGER.info("New path created: " + fileName);
						if (fileName.endsWith(".xml")) {
							if (fileLockChecker.isLocked(fileName)) {
								lockedFilesQueue.add(fileName);
								LOGGER.info(fileName + " added to locked processing map");
								LOGGER.info("Total unprocessed files in queue = " + lockedFilesQueue.size());
							} else {
								LOGGER.info(fileName + " is not locked, processing");
								KafkaJob kafkaJob = new KafkaJob(fileName);
								try {
									threadPool.submit(kafkaJob);
								} catch (RejectedExecutionException ree) {
									LOGGER.error("Can't submit processing of " + fileName + " to Kafka!");
									LOGGER.error(ExceptionUtils.getExceptionStackTrace(ree));
								}
							}
						} else {
							LOGGER.error("Non xml file detected " + fileName + "!");
						}
					 }
				}

				if (!key.reset()) {
                    break; // loop
                }
			}
		} catch (IOException | InterruptedException ioe) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
		}
	}

	public static void main(String[] argv) {

		initLog4j(1);
		if (argv.length == 0) {
			printUsage(OraaudKafka.class.getCanonicalName(), 2);
		}
		loadProps(argv[0], 3);

		String watchedPath = props.getProperty("a2.watched.path").trim();
		if ("".equals(watchedPath) || watchedPath == null) {
			LOGGER.error("watched.path parameter must set in configuration file " + argv[0]);
			LOGGER.error("Exiting.");
			System.exit(4);
		}
		// Sanity check - Check if path is a folder
		final Path watchedPathNio = Paths.get(watchedPath);
		try {
			Boolean isFolder = (Boolean) Files.getAttribute(watchedPathNio, "basic:isDirectory", LinkOption.NOFOLLOW_LINKS);
			if (!isFolder) {
				LOGGER.error("watched.path parameter " + watchedPathNio + " is not a folder");
				LOGGER.error("Exiting.");
				System.exit(4);
			}
		} catch (IOException ioe) {
			// Folder does not exists
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
			LOGGER.error("watched.path parameter " + watchedPathNio + " not exist!");
			LOGGER.error("Exiting.");
			System.exit(4);
		}

		parseThreadCount(argv[0], 5);

		int lockedQueueRefreshInterval = LOCKED_QUEUE_REFRESH_INTERVAL;
		String lockedQueueRefreshString = props.getProperty("a2.locked.file.query.interval");
		if (lockedQueueRefreshString != null && !"".equals(lockedQueueRefreshString)) {
			try {
				lockedQueueRefreshInterval = Integer.parseInt(lockedQueueRefreshString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.locked.file.query.interval -> " + lockedQueueRefreshString);
				LOGGER.warn("Setting it to " + LOCKED_QUEUE_REFRESH_INTERVAL);
			}
		}

		String sendMethod = props.getProperty("a2.send.method");
		if (SEND_FILE_KAFKA_LIGHT.equalsIgnoreCase(sendMethod)) {
			LOGGER.info("Transferring data to " + sendMethod);
			sendFileMethod = SEND_FILE_KAFKA_LIGHT_INT;
			parseKafkaProducerSettings(argv[0], 6, StringSerializer.class.getName());
			// Initialize connection to Kafka
			producer = new KafkaProducer<>(kafkaProps);
		} else if (SEND_FILE_KAFKA_FULL.equalsIgnoreCase(sendMethod)) {
			LOGGER.info("Transferring data to " + sendMethod);
			sendFileMethod = SEND_FILE_KAFKA_FULL_INT;
			parseKafkaProducerSettings(argv[0], 6, AuditFileKafkaSerializer.class.getName());
			// Initialize connection to Kafka
			producer = new KafkaProducer<>(kafkaProps);
		} else if (SEND_FILE_CASSANDRA.equalsIgnoreCase(sendMethod)) {
			LOGGER.info("Transferring data to " + sendMethod);
			sendFileMethod = SEND_FILE_CASSANDRA_INT;
			parseCassandraSettings(argv[0], 6);
			// Initialize connection to Cassandra cluster
			Ora2Cassandra.connect(cassandraHost, cassandraPort);
			Ora2Cassandra.initStatements();
		} else if (sendMethod == null || "".equals(sendMethod)) {
			LOGGER.error("a2.send.method parameter must set in configuration file " + argv[0]);
			LOGGER.error("Exiting.");
			System.exit(6);
		} else {
			LOGGER.error("a2.send.method set to wrong value " + sendMethod);
			LOGGER.error("Exiting.");
			System.exit(6);
		}

		if (sendFileMethod != SEND_FILE_KAFKA_LIGHT_INT) {
			// Only in this case unmarshaller required
			unmarshaller = AuditFileJAXBUnmarshallerSingleton.getInstance();
		}

		// Instantiate correct file lock checker
		//TODO
		//TODO - more OS'es and precision here!!!
		//TODO
		String osName = System.getProperty("os.name").toUpperCase();
		LOGGER.info("Running on " + osName);
		if ("AIX".equals(osName) || "LINUX".equals(osName) || "SOLARIS".equals(osName) || "SUNOS".equals(osName)) {
			fileLockChecker = new OpenFileSystemV();
			hostName = execAndGetResult("hostname");
		} else if ("WIN".contains(osName)) {
			//TODO
			//TODO Need more precise handling and testing for Windows
			//TODO
			fileLockChecker = new OpenFileGenericNio();
			hostName = execAndGetResult("hostname");
			isWinOs = true;
		} else {
			// Free BSD, HP-UX, Mac OS X
			fileLockChecker = new OpenFileGenericX();
			hostName = execAndGetResult("hostname");
		}

		// Before listening to new files populate queue with files already on FS
		try {
			lockedFilesQueue =  Files
			        .list(watchedPathNio)
			        .filter(Files::isRegularFile)
			        .filter(dirPath -> dirPath.toString().endsWith(".xml"))
			        .map(Path::toString)
			        .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
		} catch (IOException ioe) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
			LOGGER.error("unable to read directory " + watchedPathNio + " !");
			LOGGER.error("Exiting.");
			System.exit(4);
		}

		if (lockedFilesQueue == null) {
			lockedFilesQueue = new ConcurrentLinkedQueue<>();
		} else {
			if (lockedFilesQueue.size() > 0) {
				LOGGER.info("Total of " + lockedFilesQueue.size() + " unprocessed file added to queue.");
			}
		}

		// Add special shutdown thread
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				shutdown();
			}
		});

		ThreadFactory daemonFactory = new DaemonThreadFactory();
		// Additional single thread for checking closed files queue
		ProcessLockedFilesMap mapProcessor = new ProcessLockedFilesMap(); 
		ScheduledExecutorService lockedFileExecutor = Executors.newSingleThreadScheduledExecutor(daemonFactory);
		//scheduleAtFixedRate vs scheduleWithFixedDelay
		// We start after very short delay...
		lockedFileExecutor.scheduleWithFixedDelay(mapProcessor, 256, lockedQueueRefreshInterval, TimeUnit.MILLISECONDS);

		// Initialize main thread pool
		//TODO
		//TODO Queue size???
		//TODO 
		BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(2 * workerThreadCount);
		threadPool = new ThreadPoolExecutor(
				workerThreadCount,	// core pool size
				workerThreadCount,	// maximum pool size
				15,	// If the pool currently has more than corePoolSize threads, excess threads will be terminated if they have been idle for more than the keepAliveTime
				TimeUnit.SECONDS,	// Time unit for keep-alive
				blockingQueue	// Job queue
				);
		// Throw RejectedExecutionException with full queue
		threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());

		// Start watching for new files
		watchDirectoryPath(watchedPathNio);

	}

	private static void shutdown() {
		LOGGER.info("Shutting down...");
		//TODO - more information about processing
	}

	private static String execAndGetResult(String command) {
		String result = null;
		try (Scanner scanner = new Scanner(Runtime.getRuntime().exec(command).getInputStream()).useDelimiter("\\A")) {
			result = scanner.hasNext() ? scanner.next() : "";
		} catch (IOException e) {
			LOGGER.error("Can't execute OS command:" + command);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
		}
		return result;
	}

	private static class ProcessLockedFilesMap implements Runnable {
		@Override
		public void run() {
			Iterator<String> lockedFilesIterator = lockedFilesQueue.iterator();
			while(lockedFilesIterator.hasNext()) {
				String fileName = lockedFilesIterator.next();
				try {
//					if (!isFileLocked(new File(entry.getValue()))) {
					if (!fileLockChecker.isLocked(fileName)) {
						LOGGER.info(fileName + " is not locked, processing");
						Runnable processJob = null;
						if (sendFileMethod == SEND_FILE_KAFKA_LIGHT_INT || sendFileMethod == SEND_FILE_KAFKA_FULL_INT) {
							processJob = new KafkaJob(fileName);
						} else if (sendFileMethod == SEND_FILE_CASSANDRA_INT) {
							processJob = new CassandraJob(fileName);
						}
						try {
							threadPool.submit(processJob);
						} catch (RejectedExecutionException ree) {
							LOGGER.error("Can't submit processing of " + fileName + " to Kafka!");
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(ree));
							// We need to break cycle here
							break;
						}
						lockedFilesIterator.remove();
					}
				} catch (IOException e) {
					LOGGER.error("Exception while processing " + fileName + "!\n" +
							ExceptionUtils.getExceptionStackTrace(e));
				}
			}
		}
	}

	private static class KafkaJob implements Runnable {

		final String fileName;

		KafkaJob(String fileName) {
			this.fileName = fileName;
		}

		@Override
		public void run() {
			try {
				ProducerRecord<String, Object> record = null;
				final File auditFile = new File(fileName);
				String kafkaKey = hostName + ":" + fileName;
				if (sendFileMethod == SEND_FILE_KAFKA_FULL_INT) {
					final Audit audit = unmarshaller.getFileAsPOJO(auditFile);
					record = new ProducerRecord<>(kafkaTopic, kafkaKey, audit);
				} else if (sendFileMethod == SEND_FILE_KAFKA_LIGHT_INT) {
					
					record = new ProducerRecord<>(kafkaTopic, kafkaKey, auditFile.length() + "");
				}
				producer.send(
						record,
						(metadata, exception) -> {
							if (metadata == null) {
								// Error occured
								LOGGER.error("Exception while sending " + fileName + " to Kafka." );
								LOGGER.error(ExceptionUtils.getExceptionStackTrace(exception));
							} else {
								if (sendFileMethod == SEND_FILE_KAFKA_FULL_INT) {
									//Delete file
									try {
										Files.delete(auditFile.toPath());
									} catch (IOException ioe) {
										LOGGER.error("Exception while deleting " + fileName + "!!!" );
										LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
									}
								}
							}
						});

			} catch (JAXBException je) {
				LOGGER.error("Exception while unmarshaling " + fileName + "!!!" );
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(je));
			}
		}
	}

	private static class CassandraJob implements Runnable {

		final String fileName;

		CassandraJob(String fileName) {
			this.fileName = fileName;
		}

		@Override
		public void run() {
			try {
				File auditFile = new File(fileName);
				final Audit audit = unmarshaller.getFileAsPOJO(auditFile);
				if (Ora2Cassandra.write(fileName, audit, hostName)) {
					//Delete file
					try {
						Files.delete(auditFile.toPath());
					} catch (IOException ioe) {
						LOGGER.error("Exception while deleting " + fileName + "!!!" );
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
					}
				} else {
					LOGGER.error("Errors while sending " + fileName + " to Cassandra!!!");
				}
			} catch (JAXBException je) {
				LOGGER.error("Exception while unmarshaling " + fileName + "!!!" );
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(je));
			}
		}
	}

}
