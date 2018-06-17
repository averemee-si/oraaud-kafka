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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DaemonThreadFactory implements ThreadFactory {

	private final ThreadFactory factory;

	public DaemonThreadFactory() {
		this(Executors.defaultThreadFactory());
	}

	public DaemonThreadFactory(ThreadFactory factory) {
		if (factory == null) {
			throw new NullPointerException("Thread factory cannot be null");
		}
		this.factory = factory;
	}

	@Override
	public Thread newThread(Runnable r) {
		final Thread t = factory.newThread(r);
		t.setDaemon(true);
		return t;
	}

}
