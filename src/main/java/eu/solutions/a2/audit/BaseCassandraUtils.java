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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public abstract class BaseCassandraUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(BaseCassandraUtils.class);

	/** Cassandra Cluster. */
	protected static Cluster cluster;

	/** Cassandra Session. */
	protected static Session session;

	/** Cassandra PreparedStatements (ThreadSafe - http://docs.datastax.com/en/developer/java-driver/3.3/manual/statements/prepared/ */
	protected static PreparedStatement insertAuditAbt;
	protected static PreparedStatement insertAuditByPolicy;
	protected static PreparedStatement insertAuditByObject;
	protected static PreparedStatement insertAuditByDay;

	/**
	 * Connect to Cassandra Cluster specified by provided node IP
	 * address and port number.
	 *
	 * @param node Cluster node IP address.
	 * @param port Port of cluster host.
	 */
	public static void connect(final String node, final int port) {
		cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
		final Metadata metadata = cluster.getMetadata();
		LOGGER.info("Connected to cluster: " + metadata.getClusterName() + "\n");
		session = cluster.connect();
	}

}
