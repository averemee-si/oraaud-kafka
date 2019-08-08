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

import java.util.Properties;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
	
public class KinesisSingleton {

	private static final Logger LOGGER = Logger.getLogger(KinesisSingleton.class);

	private static KinesisSingleton instance;

	/** Kinesis stream name */
	private String streamName = null;
	/**  Kinesis producer */
	private KinesisProducer kinesisProducer;
	/** a2.kinesis.file.size.threshold */
	private int fileSizeThreshold = 512;

	private KinesisSingleton() {
	}

	public static KinesisSingleton getInstance() {
		if (instance == null) {
			instance = new KinesisSingleton();
		}
		return instance;
	}

	public String topic() {
		return streamName;
	}

	public int getFileSizeThreshold() {
		return fileSizeThreshold;
	}

	public KinesisProducer producer() {
		return kinesisProducer;
	}

	public void shutdown() {
		if (kinesisProducer != null) {
			kinesisProducer.flushSync();
			kinesisProducer.destroy();
		} else {
			LOGGER.fatal("Attempt to close non-initialized Kinesis producer!");
			System.exit(1);
		}
	}

	public void parseSettings(final Properties props, final String configPath, final int exitCode) {
		streamName = props.getProperty("a2.kinesis.stream", "");
		if (streamName == null || "".equals(streamName)) {
			LOGGER.fatal("a2.kinesis.stream parameter must set in configuration file " + configPath);
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}

		String region = props.getProperty("a2.kinesis.region", "");
		if (region == null || "".equals(region)) {
			LOGGER.fatal("a2.kinesis.region parameter must set in configuration file " + configPath);
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}

		String accessKey = props.getProperty("a2.kinesis.access.key", "");
		if (accessKey == null || "".equals(accessKey)) {
			LOGGER.fatal("a2.kinesis.access.key parameter must set in configuration file " + configPath);
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}

		String accessSecret = props.getProperty("a2.kinesis.access.secret", "");
		if (accessSecret == null || "".equals(accessSecret)) {
			LOGGER.fatal("a2.kinesis.access.secret parameter must set in configuration file " + configPath);
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}

		BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, accessSecret);
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion(region);
		config.setCredentialsProvider(new AWSStaticCredentialsProvider(awsCreds));

		// The maxConnections parameter can be used to control the degree of
        // parallelism when making HTTP requests.
		int maxConnections = 1;
		String maxConnectionsString = props.getProperty("a2.kinesis.max.connections", "");
		if (maxConnectionsString != null && !"".equals(maxConnectionsString)) {
			try {
				maxConnections = Integer.parseInt(maxConnectionsString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.kinesis.max.connections -> " + maxConnectionsString);
				LOGGER.warn("Setting it to 1");
			}
		}
		config.setMaxConnections(maxConnections);

		// Request timeout milliseconds
		long requestTimeout = 30000;
		String requestTimeoutString = props.getProperty("a2.kinesis.request.timeout", "");
		if (requestTimeoutString != null && !"".equals(requestTimeoutString)) {
			try {
				requestTimeout = Integer.parseInt(requestTimeoutString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.kinesis.request.timeout -> " + requestTimeoutString);
				LOGGER.warn("Setting it to 30000");
			}
		}
		config.setRequestTimeout(requestTimeout);

		// RecordMaxBufferedTime
		long recordMaxBufferedTime = 5000;
		String recordMaxBufferedTimeString = props.getProperty("a2.kinesis.request.record.max.buffered.time", "");
		if (recordMaxBufferedTimeString != null && !"".equals(recordMaxBufferedTimeString)) {
			try {
				recordMaxBufferedTime = Integer.parseInt(recordMaxBufferedTimeString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.kinesis.request.record.max.buffered.time -> " + recordMaxBufferedTimeString);
				LOGGER.warn("Setting it to 5000");
			}
		}
		config.setRecordMaxBufferedTime(recordMaxBufferedTime);

		// fileSizeThreshold
		String fileSizeThresholdString = props.getProperty("a2.kinesis.file.size.threshold", "");
		if (fileSizeThresholdString != null && !"".equals(fileSizeThresholdString)) {
			try {
				fileSizeThreshold = Integer.parseInt(fileSizeThresholdString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.kinesis.file.size.threshold -> " + fileSizeThresholdString);
				LOGGER.warn("Setting it to 512");
			}
		}

		// Initialize connection to Kinesis
		kinesisProducer = new KinesisProducer(config);
	}


}
