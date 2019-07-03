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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class CommonJobMgmt implements CommonJobMgmtMBean {

	private final long startTimeMillis = System.currentTimeMillis();
	private final AtomicLong filesCount = new AtomicLong(0L);
	private final AtomicLong filesSize = new AtomicLong(0L);
	private final AtomicLong processingTime = new AtomicLong(0L);

	public void addFileData(long fileSize, long elapsedMillis) {
		filesCount.incrementAndGet();
		filesSize.addAndGet(fileSize);
		processingTime.addAndGet(elapsedMillis);
	}

	@Override
	public long getElapsedTimeMillis() {
		return System.currentTimeMillis() - startTimeMillis;
	}

	@Override
	public String getElapsedTime() {
		Duration duration = Duration.ofMillis(System.currentTimeMillis() - startTimeMillis);
		return String.format("%sdays %shrs %smin %ssec.\n",
				duration.toDays(),
				duration.toHours() % 24,
				duration.toMinutes() % 60,
				duration.getSeconds() % 60);
	}

	@Override
	public long getFilesCount() {
		return filesCount.longValue();
	}

	@Override
	public long getFilesSize() {
		return filesSize.longValue();
	}

	@Override
	public long getTransferTimeMillis() {
		return processingTime.longValue();
	}

	@Override
	public String getTransferTime() {
		Duration duration = Duration.ofMillis(processingTime.longValue());
		return String.format("%sdays %shrs %smin %ssec.\n",
				duration.toDays(),
				duration.toHours() % 24,
				duration.toMinutes() % 60,
				duration.getSeconds() % 60);
	}

}
