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

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import eu.solutions.a2.audit.ora112.schema.Audit;
import eu.solutions.a2.audit.utils.ExceptionUtils;

public class AuditFileKafkaSerializer implements Serializer<Audit> {

	private static final Logger LOGGER = LoggerFactory.getLogger(AuditFileKafkaSerializer.class);
	private static final ObjectWriter writer = new ObjectMapper()
									.setSerializationInclusion(Include.NON_NULL)
									.writer();

	@Override
	public byte[] serialize(String topic, Audit data) {
		try {
			return writer.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
		}
		return null;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub		
	}

}
