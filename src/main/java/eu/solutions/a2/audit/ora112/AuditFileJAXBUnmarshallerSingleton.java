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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.UnmarshalException;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXParseException;

import eu.solutions.a2.audit.ora112.schema.Audit;
import eu.solutions.a2.audit.utils.ExceptionUtils;

public class AuditFileJAXBUnmarshallerSingleton {

	private static final Logger LOGGER = LoggerFactory.getLogger(AuditFileJAXBUnmarshallerSingleton.class);
	private static AuditFileJAXBUnmarshallerSingleton instance = null;
	private final JAXBContext jaxbContext; 

	public AuditFileJAXBUnmarshallerSingleton() throws JAXBException {
		jaxbContext = JAXBContext.newInstance(Audit.class);
	}

	public static synchronized AuditFileJAXBUnmarshallerSingleton getInstance() {
		if (instance == null) {
			try {
				instance = new AuditFileJAXBUnmarshallerSingleton();
			} catch (JAXBException e) {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			}
		}
		return instance;
	}

	public Audit getFileAsPOJO(File file) throws JAXBException {
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		Audit audit = null;
		try {
			audit = (Audit) unmarshaller.unmarshal(file);
		} catch (Exception ue) {
			if (ue instanceof UnmarshalException) {
				if (((UnmarshalException) ue).getLinkedException() instanceof SAXParseException &&
						"XML document structures must start and end within the same entity."
							.equals(((UnmarshalException) ue).getLinkedException().getMessage())) {
					// Usualy we have valid Oracle xml without trailing </Audit> tag
					String xmlFileContent = null;
					try {
						byte[] baXmlFile = Files.readAllBytes(file.toPath());
						xmlFileContent = new String(baXmlFile, "UTF-8");
					} catch (IOException ioe) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
					}
					if (xmlFileContent.trim().endsWith("</AuditRecord>")) {
						xmlFileContent += "</Audit>";
						try (InputStream is = new ByteArrayInputStream(xmlFileContent.getBytes("UTF-8"))) {
							audit = (Audit) unmarshaller.unmarshal(is);
						} catch (Exception secondLevel) {
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(secondLevel));
						}
					} else {
						throw new JAXBException(ue);
					}
				} else {
					throw new JAXBException(ue);
				}
			} else {
				throw new JAXBException(ue);
			}
		}
		return audit;
	}

/*
	public static void main(String[] argv) {
		try {
			AuditFileJAXBUnmarshallerSingleton instance = AuditFileJAXBUnmarshallerSingleton.getInstance();
			Audit audit = instance.getFileAsPOJO(new File("/Users/alessio/EBSDB_ora_25754_20171028072724745689205367.xml"));
			System.err.println(audit.getVersion());
			System.err.println(audit.getAuditRecord().size());
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
*/
}
