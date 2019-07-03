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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import eu.solutions.a2.audit.utils.ExceptionUtils;
import eu.solutions.a2.audit.utils.GzipUtil;

public class KinesisJob implements Callable<Void> {

	private static final Logger LOGGER = Logger.getLogger(KinesisJob.class);

	final OraTrcNameHolder trcFile;
	final CommonJobSingleton commonData;
	final KinesisSingleton kinesisData;

	KinesisJob(OraTrcNameHolder trcFile) {
		this.trcFile = trcFile;
		this.commonData = CommonJobSingleton.getInstance();
		this.kinesisData = KinesisSingleton.getInstance();
	}

	@Override
	public Void call() {
		try {
			long startTime = System.currentTimeMillis();
			final File auditFile = new File(trcFile.fileName);
			String kinesisKey = commonData.getHostName() + ":" + trcFile.fileName;

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
				byte[] valueBytes = null; 
				int fileSize = value.getBytes().length;
				if (fileSize > kinesisData.getFileSizeThreshold()) {
					// Compress file data
					valueBytes = GzipUtil.compress(value);
				} else {
					valueBytes = value.getBytes("UTF-8");
				}

				ListenableFuture<UserRecordResult> futureResult =
						kinesisData.producer().addUserRecord(
								kinesisData.topic(), kinesisKey, ByteBuffer.wrap(valueBytes));
				Futures.addCallback(futureResult,
						new KinesisAsyncCallback(auditFile, fileSize, startTime));
				
			} else {
				LOGGER.error("Mailformed or incomplete xml file: " + trcFile.fileName);
				if (commonData.getLockedFiles().contains(trcFile)) {
					LOGGER.error("Unhanled concurrency issue with xml file: " + trcFile.fileName);
				} else {
					commonData.addFileToQueue(trcFile);
				}
			}
		} catch (IOException ioe) {
			LOGGER.error("Exception while processing " + trcFile.fileName + "!!!" );
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
		}
		return null;
	}

	private class KinesisAsyncCallback implements FutureCallback<UserRecordResult> {

		private final File auditFile;
		private final int fileSize;
		private final long startTime;

		KinesisAsyncCallback(final File auditFile, final int fileSize, final long startTime) {
			this.auditFile = auditFile;
			this.fileSize = fileSize;
			this.startTime = startTime;
		}
		
		@Override
		public void onSuccess(UserRecordResult result) {
			//Delete file
			try {
				Files.delete(auditFile.toPath());
				commonData.addFileData(
						fileSize,
						System.currentTimeMillis() - startTime);
			} catch (IOException ioe) {
				LOGGER.error("Exception while deleting " + trcFile.fileName + "!!!" );
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
			}
		}

		@Override
		public void onFailure(Throwable t) {
			LOGGER.error("Exception while processing " + trcFile.fileName + "!!!" );
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(new Exception(t)));
		}
	}
}
