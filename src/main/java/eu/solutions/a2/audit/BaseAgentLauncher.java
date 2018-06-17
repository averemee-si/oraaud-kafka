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

package eu.solutions.a2.audit;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.audit.utils.ExceptionUtils;

public abstract class BaseAgentLauncher {

	private static final Logger LOGGER = LoggerFactory.getLogger(BaseAgentLauncher.class);

	protected static final Properties props = new Properties();
	/** Default number of worker threads */
	protected static final int WORKER_THREAD_COUNT = 16;
	/** Maximum number of worker threads */
	protected static final int WORKER_THREAD_MAX = 150;
	/** Number of async workers for data transfer */
	protected static int workerThreadCount = WORKER_THREAD_COUNT;

	/** Proprties for building Kafka producer */
	protected static Properties kafkaProps;
	/** Kafka topic */
	public static String kafkaTopic = null;

	/** Cassandra parameters */
	protected static String cassandraHost = null;
	protected static int cassandraPort = -1;

	protected static void initLog4j(int exitCode) {
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

	protected static void printUsage(String className, int exitCode) {
		LOGGER.error("Usage:\njava " + className + " <full path to configuration file>");
		LOGGER.error("Exiting.");
		System.exit(exitCode);
	}

	protected static void loadProps(String configPath, int exitCode) {
		try {
			props.load(new FileInputStream(configPath));
		} catch (IOException eoe) {
			LOGGER.error("Unable to open configuration file " + configPath);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(eoe));
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}
	}

	protected static void parseThreadCount(String configPath, int exitCode) {
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

	protected static void parseKafkaProducerSettings(String configPath, int exitCode, String kafkaSerializer) {
		kafkaTopic = props.getProperty("a2.kafka.topic");
		if (kafkaTopic == null || "".equals(kafkaTopic)) {
			LOGGER.error("kafka.topic parameter must set in configuration file " + configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}

		String kafkaServers = props.getProperty("a2.kafka.servers");
		if (kafkaServers == null || "".equals(kafkaServers)) {
			LOGGER.error("kafka.servers parameter must set in configuration file " + configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}

		String kafkaClientId = props.getProperty("a2.kafka.client.id");
		if (kafkaServers == null || "".equals(kafkaServers)) {
			LOGGER.error("a2.kafka.client.id parameter must set in configuration file " + configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}

		//
		//TODO - hardcoding!!!
		//
		int kafkaMaxRequestSize = 11010048;
		int kafkaBatchSize = 256;

		kafkaProps = new Properties();
		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaClientId);
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaSerializer);
		kafkaProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaMaxRequestSize);
		kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaBatchSize);
		/** Always try to use compression */
//		kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, 1);
	}

	protected static void parseCassandraSettings(String configPath, int exitCode) {
		cassandraHost = props.getProperty("a2.cassandra.host");
		if (cassandraHost == null || "".equals(cassandraHost)) {
			LOGGER.error("a2.cassandra.host parameter must set in configuration file " + configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}
		String cassandraPortString = props.getProperty("a2.cassandra.port");
		if (cassandraPortString == null || "".equals(cassandraPortString)) {
			LOGGER.error("a2.cassandra.port parameter must set in configuration file " + configPath);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}
		try {
			cassandraPort = Integer.parseInt(cassandraPortString);
		} catch (Exception e) {
			LOGGER.error("Incorrect value for a2.cassandra.port -> " + cassandraPortString);
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}
	}
}
