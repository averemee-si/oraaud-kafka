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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import eu.solutions.a2.audit.utils.ExceptionUtils;
import eu.solutions.a2.audit.utils.file.OpenFileGenericNio;
import eu.solutions.a2.audit.utils.file.OpenFileGenericX;
import eu.solutions.a2.audit.utils.file.OpenFileSystemV;
import eu.solutions.a2.audit.utils.file.OpenFilesIntf;

public class OraaudKafka {

	private static final Logger LOGGER = Logger.getLogger(OraaudKafka.class);

	/**   Default interval in milliseconds of locked file queue refresh */
	private static final int LOCKED_QUEUE_REFRESH_INTERVAL = 1000;

	private static OpenFilesIntf fileLockChecker = null;

	private static final Properties props = new Properties();
	/** Supported target systems */
	private static final int TARGET_KAFKA = 0;
	private static final int TARGET_KINESIS = 1;
	/** Set default target system to Apache Kafka */
	private static int targetSystem = TARGET_KAFKA;
	/** Default number of worker threads */
	private static final int WORKER_THREAD_COUNT = 16;
	/** Maximum number of worker threads */
	private static final int WORKER_THREAD_MAX = 150;
	/** Number of async workers for data transfer */
	private static int workerThreadCount = WORKER_THREAD_COUNT;
	/**   Main thread pool for data tansfer jobs */
	private static ThreadPoolExecutor threadPool;
	/** Windows OS? **/
	private static boolean isWinOs = false;

	//TODO
	//TODO - add MB https://www.ibm.com/developerworks/ru/library/j-jtp09196/index.html
	//TODO
	public static void watchDirectoryPath(final Path watchedPathNio, final CommonJobSingleton commonData) {

		String watchedDir = watchedPathNio.toAbsolutePath().toString() + File.separator;
		LOGGER.info("Watching path: " + watchedPathNio.toString());

		// We obtain the file system of the Path
		final FileSystem fs = watchedPathNio.getFileSystem();

		try (WatchService service = fs.newWatchService()) {
			// We're interrested only in new files!
			watchedPathNio.register(service, StandardWatchEventKinds.ENTRY_CREATE);
			//TODO - https://www.programcreek.com/java-api-examples/?class=java.nio.file.WatchEvent&method=Modifier

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
						LOGGER.error("File listener recieved an overflow event!\n");
						commonData.initLockedFilesQueue(watchedPathNio);
						//TODO
						//TODO - more actions???
						//TODO
						continue; // loop
					 } else if (StandardWatchEventKinds.ENTRY_CREATE == kind) {
						// A new Path was created
						@SuppressWarnings("unchecked")
						Path newPath = ((WatchEvent<Path>) watchEvent).context();
						String fileName = watchedDir + newPath.getFileName(); 
						LOGGER.info("New path created: " + fileName);
						if (fileName.endsWith(".xml")) {
							commonData.addFileToQueue(new OraTrcNameHolder(fileName));
							LOGGER.info(fileName + " added to locked processing map");
							LOGGER.warn("Total unprocessed files in queue = " + commonData.getLockedFiles().size());
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

		String targetBroker = props.getProperty("a2.target.broker", "kafka").trim();
		if ("kafka".equalsIgnoreCase(targetBroker)) {
			targetSystem = TARGET_KAFKA;
		} else if ("kinesis".equalsIgnoreCase(targetBroker)) {
			targetSystem = TARGET_KINESIS;
		} else {
			LOGGER.fatal("wrong target broker type specified in configuration file " + argv[0]);
			LOGGER.fatal("Exiting.");
			System.exit(3);
		}

		String watchedPath = props.getProperty("a2.watched.path").trim();
		if ("".equals(watchedPath) || watchedPath == null) {
			LOGGER.fatal("watched.path parameter must set in configuration file " + argv[0]);
			LOGGER.fatal("Exiting.");
			System.exit(4);
		}
		// Sanity check - Check if path is a folder
		final Path watchedPathNio = Paths.get(watchedPath);
		try {
			Boolean isFolder = (Boolean) Files.getAttribute(watchedPathNio, "basic:isDirectory", LinkOption.NOFOLLOW_LINKS);
			if (!isFolder) {
				LOGGER.fatal("watched.path parameter " + watchedPathNio + " is not a folder");
				LOGGER.fatal("Exiting.");
				System.exit(4);
			}
		} catch (IOException ioe) {
			// Folder does not exists
			LOGGER.fatal(ExceptionUtils.getExceptionStackTrace(ioe));
			LOGGER.fatal("watched.path parameter " + watchedPathNio + " not exist!");
			LOGGER.fatal("Exiting.");
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

		// Instantiate correct file lock checker
		//TODO
		//TODO - more OS'es and precision here!!!
		//TODO
		String osName = System.getProperty("os.name").toUpperCase();
		LOGGER.info("Running on " + osName);
		if ("AIX".equals(osName) || "LINUX".equals(osName) || "SOLARIS".equals(osName) || "SUNOS".equals(osName)) {
			fileLockChecker = new OpenFileSystemV();
		} else if ("WIN".contains(osName)) {
			//TODO
			//TODO Need more precise handling and testing for Windows
			//TODO
			fileLockChecker = new OpenFileGenericNio();
			isWinOs = true;
		} else {
			// Free BSD, HP-UX, Mac OS X
			fileLockChecker = new OpenFileGenericX();
		}

		final CommonJobSingleton commonData = CommonJobSingleton.getInstance();

		if (targetSystem == TARGET_KAFKA) {
			KafkaSingleton.getInstance().parseSettings(props, argv[0], 6);
		} else if (targetSystem == TARGET_KINESIS) {
			KinesisSingleton.getInstance().parseSettings(props, argv[0], 6);
		}

		// Add special shutdown thread
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (targetSystem == TARGET_KAFKA) {
					KafkaSingleton.getInstance().shutdown();
				} else if (targetSystem == TARGET_KINESIS) {
					KinesisSingleton.getInstance().shutdown();
				}
				LOGGER.info("Shutting down...");
				//TODO - more information about processing
			}
		});

		// Before listening to new files populate queue with files already on FS
		try {
			commonData.initLockedFilesQueue(watchedPathNio);
		} catch (IOException ioe) {
			LOGGER.fatal(ExceptionUtils.getExceptionStackTrace(ioe));
			LOGGER.fatal("unable to read directory " + watchedPathNio + " !");
			LOGGER.fatal("Exiting.");
			System.exit(4);
		}

		if (commonData.getLockedFiles() == null) {
			commonData.initLockedFilesQueue();
		} else {
			if (commonData.getLockedFiles().size() > 0) {
				LOGGER.info("Total of " + commonData.getLockedFiles().size() + " unprocessed file added to queue.");
			}
		}

		// Additional single thread for checking closed files queue
		ProcessLockedFilesMap mapProcessor = new ProcessLockedFilesMap(); 
		ScheduledExecutorService lockedFileExecutor = Executors.newSingleThreadScheduledExecutor(
				new ThreadFactory() {
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory().newThread(r);
						t.setDaemon(true);
						return t;
					}
				});
		//scheduleAtFixedRate vs scheduleWithFixedDelay
		// We start after very short delay...
		lockedFileExecutor.scheduleWithFixedDelay(mapProcessor, 256, lockedQueueRefreshInterval, TimeUnit.MILLISECONDS);

		// Initialize main thread pool
		BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(4096 * workerThreadCount);
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
		watchDirectoryPath(watchedPathNio, commonData);

	}

	private static class ProcessLockedFilesMap implements Runnable {
		final CommonJobSingleton commonData;
		ProcessLockedFilesMap() {
			this.commonData = CommonJobSingleton.getInstance();
		}
		@Override
		public void run() {
			Iterator<OraTrcNameHolder> lockedFilesIterator = commonData.getLockedFiles().iterator();
			while (lockedFilesIterator.hasNext()) {
				OraTrcNameHolder trcFile = lockedFilesIterator.next();
				try {
//					if (!isFileLocked(new File(entry.getValue()))) {
					if (!fileLockChecker.isLocked(trcFile.pid, trcFile.fileName)) {
						LOGGER.info(trcFile.fileName + " is not locked, processing");
						Callable<Void> processJob = null;
						if (targetSystem == TARGET_KAFKA) {
							processJob = new KafkaJob(trcFile);
						} else if (targetSystem == TARGET_KINESIS) {
							processJob = new KinesisJob(trcFile);
						}
						try {
							threadPool.submit(processJob);
						} catch (RejectedExecutionException ree) {
							LOGGER.error("Can't submit processing of " + trcFile.fileName + " to Kafka!");
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(ree));
							// We need to break cycle here
							break;
						}
						//TODO remove here or in job???
						lockedFilesIterator.remove();
					}
				} catch (IOException e) {
					LOGGER.error("Exception while processing " + trcFile.fileName + "!\n" +
							ExceptionUtils.getExceptionStackTrace(e));
				}
			}
		}
	}

	private static void initLog4j(int exitCode) {
		// Check for valid log4j configuration
		String log4jConfig = System.getProperty("a2.log4j.configuration");
		if (log4jConfig == null || "".equals(log4jConfig)) {
			System.err.println("JVM argument -Da2.log4j.configuration must set!");
			System.err.println("Exiting.");
			System.exit(exitCode);
		}

		// Check that log4j configuration file exist
		Path path = Paths.get(log4jConfig);
		if (!Files.exists(path) || Files.isDirectory(path)) {
			System.err.println("JVM argument -Da2.log4j.configuration points to unknown file " + log4jConfig + "!");
			System.err.println("Exiting.");
			System.exit(exitCode);
		}
		// Initialize log4j
		PropertyConfigurator.configure(log4jConfig);

	}

	private static void printUsage(String className, int exitCode) {
		LOGGER.fatal("Usage:\njava " + className + " <full path to configuration file>");
		LOGGER.fatal("Exiting.");
		System.exit(exitCode);
	}

	private static void loadProps(String configPath, int exitCode) {
		try {
			props.load(new FileInputStream(configPath));
		} catch (IOException eoe) {
			LOGGER.fatal("Unable to open configuration file " + configPath);
			LOGGER.fatal(ExceptionUtils.getExceptionStackTrace(eoe));
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}
	}

	private static void parseThreadCount(String configPath, int exitCode) {
		String threadCountString = props.getProperty("a2.worker.count").trim();
		if (threadCountString != null && !"".equals(threadCountString)) {
			try {
				workerThreadCount = Integer.parseInt(threadCountString);
			} catch (Exception e) {
				LOGGER.error("a2.worker.count set to wrong value in " + configPath);
				LOGGER.error("Exiting.");
				System.exit(exitCode);
			}
			if (workerThreadCount > WORKER_THREAD_MAX) {
				LOGGER.warn("a2.worker.count is maximum that allowed. Setting it to " + WORKER_THREAD_MAX);
				workerThreadCount = WORKER_THREAD_MAX;
			} else if (workerThreadCount < 0) {
				LOGGER.warn("a2.worker.count is negative. Setting it to " + WORKER_THREAD_COUNT);
				workerThreadCount = WORKER_THREAD_COUNT;
			}
		}
	}


}
