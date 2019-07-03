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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import eu.solutions.a2.audit.utils.ExceptionUtils;

public class KafkaJob implements Callable<Void> {

	private static final Logger LOGGER = Logger.getLogger(KafkaJob.class);

	final OraTrcNameHolder trcFile;
	final CommonJobSingleton commonData;
	final KafkaSingleton kafkaData;

	KafkaJob(OraTrcNameHolder trcFile) {
		this.trcFile = trcFile;
		this.commonData = CommonJobSingleton.getInstance();
		this.kafkaData = KafkaSingleton.getInstance();
	}

	@Override
	public Void call() {
		try {
			long startTime = System.currentTimeMillis();
			final File auditFile = new File(trcFile.fileName);
			String kafkaKey = commonData.getHostName() + ":" + trcFile.fileName;

			BufferedReader reader = new BufferedReader(new FileReader(auditFile));
			String line = null;
			StringBuilder sb = new StringBuilder();
			while ((line = reader.readLine()) != null) {
				sb.append(line);
//				sb.append("\n");
			}
			reader.close();
			String value = sb.toString();

			if (value.trim().endsWith("</Audit>")) {
				ProducerRecord<String, String> record = new ProducerRecord<>(kafkaData.topic(), kafkaKey, value);
				kafkaData.producer().send(
					record,
					(metadata, exception) -> {
						if (metadata == null) {
							// Error occured
							LOGGER.error("Exception while sending " + trcFile.fileName + " to Kafka." );
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(exception));
						} else {
							//Delete file
							try {
								Files.delete(auditFile.toPath());
								commonData.addFileData(
										value.getBytes().length,
										System.currentTimeMillis() - startTime);
							} catch (IOException ioe) {
								LOGGER.error("Exception while deleting " + trcFile.fileName + "!!!" );
								LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
							}
						}
					});
			} else {
				LOGGER.error("Mailformed or incomplete xml file: " + trcFile.fileName);
				if (commonData.getLockedFiles().contains(trcFile)) {
					LOGGER.error("Unhanled concurrency issue with xml file: " + trcFile.fileName);
				} else {
					commonData.addFileToQueue(trcFile);
				}
			}
		} catch (IOException je) {
			LOGGER.error("Exception while processing " + trcFile.fileName + "!!!" );
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(je));
		}
		return null;
	}

}
