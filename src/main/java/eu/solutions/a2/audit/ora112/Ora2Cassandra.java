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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import eu.solutions.a2.audit.BaseCassandraUtils;
import eu.solutions.a2.audit.ora112.schema.Audit;
import eu.solutions.a2.audit.ora112.schema.Audit.AuditRecord;
import eu.solutions.a2.audit.utils.ExceptionUtils;

public class Ora2Cassandra extends BaseCassandraUtils {

/*
create table AUDIT_ABT(
	AUDIT_TYPE int,
	EXTENDED_TIMESTAMP timestamp,
	SERVER_HOST_NAME text,
	VERSION text,
	FILE_NAME text,
	SESSION_ID bigint,
	PROXY_SESSION_ID bigint,
	STATEMENT_ID bigint,
	ENTRY_ID bigint,
	GLOBAL_UID text,
	DB_USER text,
	CLIENT_ID text,
	EXT_NAME text,
	OS_USER text,
	USERHOST text,
	OS_PROCESS text,
	TERMINAL text,
	INSTANCE_NUMBER int,
	OBJECT_SCHEMA text,
	OBJECT_NAME text,
	POLICY_NAME text,
	NEW_OWNER text,
	NEW_NAME text,
	ACTION bigint,
	STMT_TYPE int,
	TRANSACTION_ID blob,
	RETURN_CODE bigint,
	SCN bigint,
	COMMENT_TEXT text,
	AUTH_PRIVILEGES text,
	OS_PRIVILEGE text,
	GRANTEE text,
	PRIV_USED bigint,
	SES_ACTIONS text,
	OBJ_EDITION_NAME text,
	PRIV_GRANTED bigint,
	DBID bigint,
	SQL_TEXT text,
	SQL_BIND text,
	primary key(AUDIT_TYPE, EXTENDED_TIMESTAMP, SERVER_HOST_NAME)
);

 */

	private static final Logger LOGGER = LoggerFactory.getLogger(Ora2Cassandra.class);
	private static final ObjectReader reader = new ObjectMapper().readerFor(Audit.class);
/*
insert into AUDIT.AUDIT_ABT(
	ROWID, AUDIT_TYPE, EXTENDED_TIMESTAMP, SERVER_HOST_NAME, VERSION,
	FILE_NAME, SESSION_ID, PROXY_SESSION_ID, STATEMENT_ID, ENTRY_ID,
	GLOBAL_UID, DB_USER, CLIENT_ID, EXT_NAME, OS_USER, USERHOST,
	OS_PROCESS, TERMINAL, INSTANCE_NUMBER, OBJECT_SCHEMA, OBJECT_NAME,
	POLICY_NAME, NEW_OWNER, NEW_NAME, ACTION, STMT_TYPE,
	TRANSACTION_ID, RETURN_CODE, SCN, COMMENT_TEXT, AUTH_PRIVILEGES,
	OS_PRIVILEGE, GRANTEE, PRIV_USED, SES_ACTIONS, OBJ_EDITION_NAME,
	PRIV_GRANTED, DBID, SQL_TEXT, SQL_BIND)
values(
	:ROWID, :AUDIT_TYPE, :EXTENDED_TIMESTAMP, :SERVER_HOST_NAME, :VERSION,
	:FILE_NAME, :SESSION_ID, :PROXY_SESSION_ID, :STATEMENT_ID, :ENTRY_ID,
	:GLOBAL_UID, :DB_USER, :CLIENT_ID, :EXT_NAME, :OS_USER, :USERHOST,
	:OS_PROCESS, :TERMINAL, :INSTANCE_NUMBER, :OBJECT_SCHEMA, :OBJECT_NAME,
	:POLICY_NAME, :NEW_OWNER, :NEW_NAME, :ACTION, :STMT_TYPE,
	:TRANSACTION_ID, :RETURN_CODE, :SCN, :COMMENT_TEXT, :AUTH_PRIVILEGES,
	:OS_PRIVILEGE, :GRANTEE, :PRIV_USED, :SES_ACTIONS, :OBJ_EDITION_NAME,
	:PRIV_GRANTED, :DBID, :SQL_TEXT, :SQL_BIND)

 */

 	private static final String insertCassandra = "insert into AUDIT.AUDIT_ABT(\n" +
	"	ROWID, AUDIT_TYPE, EXTENDED_TIMESTAMP, SERVER_HOST_NAME, VERSION,\n" +
	"	FILE_NAME, SESSION_ID, PROXY_SESSION_ID, STATEMENT_ID, ENTRY_ID,\n" +
	"	GLOBAL_UID, DB_USER, CLIENT_ID, EXT_NAME, OS_USER, USERHOST,\n" +
	"	OS_PROCESS, TERMINAL, INSTANCE_NUMBER, OBJECT_SCHEMA, OBJECT_NAME,\n" +
	"	POLICY_NAME, NEW_OWNER, NEW_NAME, ACTION, STMT_TYPE,\n" +
	"	TRANSACTION_ID, RETURN_CODE, SCN, COMMENT_TEXT, AUTH_PRIVILEGES,\n" +
	"	OS_PRIVILEGE, GRANTEE, PRIV_USED, SES_ACTIONS, OBJ_EDITION_NAME,\n" +
	"	PRIV_GRANTED, DBID, SQL_TEXT, SQL_BIND)\n" +
	"values(\n" +
	"	:ROWID, :AUDIT_TYPE, :EXTENDED_TIMESTAMP, :SERVER_HOST_NAME, :VERSION,\n" +
	"	:FILE_NAME, :SESSION_ID, :PROXY_SESSION_ID, :STATEMENT_ID, :ENTRY_ID,\n" +
	"	:GLOBAL_UID, :DB_USER, :CLIENT_ID, :EXT_NAME, :OS_USER, :USERHOST,\n" +
	"	:OS_PROCESS, :TERMINAL, :INSTANCE_NUMBER, :OBJECT_SCHEMA, :OBJECT_NAME,\n" +
	"	:POLICY_NAME, :NEW_OWNER, :NEW_NAME, :ACTION, :STMT_TYPE,\n" +
	"	:TRANSACTION_ID, :RETURN_CODE, :SCN, :COMMENT_TEXT, :AUTH_PRIVILEGES,\n" +
	"	:OS_PRIVILEGE, :GRANTEE, :PRIV_USED, :SES_ACTIONS, :OBJ_EDITION_NAME,\n" +
	"	:PRIV_GRANTED, :DBID, :SQL_TEXT, :SQL_BIND)";

 	private static final String insertByPolicy =
 		"insert into AUDIT.AUDIT_BY_POLICY(POLICY_NAME, ROWID) values(:POLICY_NAME, :ROWID)";
 	private static final String insertByObject =
 	 		"insert into AUDIT.AUDIT_BY_OBJECT(OBJECT_SCHEMA,OBJECT_NAME, ROWID) values(:OBJECT_SCHEMA, :OBJECT_NAME, :ROWID)";
 	private static final String insertByDay =
 	 		"insert into AUDIT.AUDIT_BY_DAY(DAY, ROWID) values(:DAY, :ROWID)";

 	public static void initStatements() {
		insertAuditAbt = session.prepare(insertCassandra);
		insertAuditByPolicy = session.prepare(insertByPolicy);
		insertAuditByObject = session.prepare(insertByObject);
		insertAuditByDay = session.prepare(insertByDay);
 	}

 	public static void write(String fileName, String jsonData, String hostname) {
		try {
			Audit audit = reader.readValue(jsonData);
			write(fileName, audit, hostname);
		} catch (IOException e) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
		}
	}

 	public static boolean write(String fileName, Audit audit, String hostname) {
 		boolean result = false;
 		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
 		try {
//			BatchStatement insertBatch = new BatchStatement();
			for (AuditRecord auditRecord : audit.getAuditRecord()) {
				UUID rowId = UUID.randomUUID();
				Date timeStamp = auditRecord.getExtendedTimestamp().toGregorianCalendar().getTime();
				String day = simpleDateFormat.format(timeStamp);

				BoundStatement bsAuditAbt = new BoundStatement(insertAuditAbt);
				// non-null values
				bsAuditAbt.setUUID("ROWID", rowId);
				bsAuditAbt.setInt("AUDIT_TYPE", auditRecord.getAuditType());
				bsAuditAbt.setTimestamp("EXTENDED_TIMESTAMP", timeStamp);
				bsAuditAbt.setString("SERVER_HOST_NAME", hostname);

				// Not null for Oracle RDBMS
				bsAuditAbt.setString("VERSION", audit.getVersion());
				// Always different name and not null for Oracle
				bsAuditAbt.setString("FILE_NAME", fileName);

				// Set to -1 if null because part of primary key
				if (auditRecord.getSessionId() != null)
					bsAuditAbt.setLong("SESSION_ID", auditRecord.getSessionId());
				else
					bsAuditAbt.setLong("SESSION_ID", -1l);

				// Nullable fields
				if (auditRecord.getProxySessionId() != null)
					bsAuditAbt.setLong("PROXY_SESSION_ID", auditRecord.getProxySessionId());
				if (auditRecord.getStatementId() != null) {
					try {
						bsAuditAbt.setLong("STATEMENT_ID", auditRecord.getStatementId().longValueExact());
					} catch (ArithmeticException ae) {
						LOGGER.warn("Can't process Statement_Id = " + auditRecord.getStatementId());
					}
				}
				if (auditRecord.getEntryId() != null) {
					try {
						bsAuditAbt.setLong("ENTRY_ID", auditRecord.getEntryId().longValueExact());
					} catch (ArithmeticException ae) {
						LOGGER.warn("Can't process Entry_Id = " + auditRecord.getEntryId());
					}
				}
				if (auditRecord.getGlobalUid() != null)
					bsAuditAbt.setString("GLOBAL_UID", auditRecord.getGlobalUid());
				if (auditRecord.getDBUser() != null)
					bsAuditAbt.setString("DB_USER", auditRecord.getDBUser());
				if (auditRecord.getClientId() != null)
					bsAuditAbt.setString("CLIENT_ID", auditRecord.getClientId());
				if (auditRecord.getExtName() != null)
					bsAuditAbt.setString("EXT_NAME", auditRecord.getExtName());
				if (auditRecord.getOSUser() != null)
					bsAuditAbt.setString("OS_USER", auditRecord.getOSUser());
				if (auditRecord.getUserhost() != null)
					bsAuditAbt.setString("USERHOST", auditRecord.getUserhost());
				if (auditRecord.getOSProcess() != null)
					bsAuditAbt.setString("OS_PROCESS", auditRecord.getOSProcess());
				if (auditRecord.getTerminal() != null)
					bsAuditAbt.setString("TERMINAL", auditRecord.getTerminal());
				if (auditRecord.getInstanceNumber() != null)
					bsAuditAbt.setInt("INSTANCE_NUMBER", auditRecord.getInstanceNumber());
				if (auditRecord.getObjectSchema() != null) {
					bsAuditAbt.setString("OBJECT_SCHEMA", auditRecord.getObjectSchema());
					if (auditRecord.getObjectName() != null) {
						/* AUDIT_BY_OBJECT */
						BoundStatement bsAuditByObject = new BoundStatement(insertAuditByObject);
						bsAuditByObject.setUUID("ROWID", rowId);
						bsAuditByObject.setString("OBJECT_SCHEMA", auditRecord.getObjectSchema());
						bsAuditByObject.setString("OBJECT_NAME", auditRecord.getObjectName());
						session.execute(bsAuditByObject);
					}
				}
				if (auditRecord.getObjectName() != null)
					bsAuditAbt.setString("OBJECT_NAME", auditRecord.getObjectName());
				if (auditRecord.getPolicyName() != null) {
					bsAuditAbt.setString("POLICY_NAME", auditRecord.getPolicyName());
					/* AUDIT_BY_POLICY */
					BoundStatement bsAuditByPolicy = new BoundStatement(insertAuditByPolicy);
					bsAuditByPolicy.setUUID("ROWID", rowId);
					bsAuditByPolicy.setString("POLICY_NAME", auditRecord.getPolicyName());
					session.execute(bsAuditByPolicy);
				}
				if (auditRecord.getNewOwner() != null)
					bsAuditAbt.setString("NEW_OWNER", auditRecord.getNewOwner());
				if (auditRecord.getNewName() != null)
					bsAuditAbt.setString("NEW_NAME", auditRecord.getNewName());
				if (auditRecord.getAction() != null) {
					try {
						bsAuditAbt.setLong("ACTION", auditRecord.getAction().longValueExact());
					} catch (ArithmeticException ae) {
						LOGGER.warn("Can't process Action = " + auditRecord.getAction());
					}
				}
				if (auditRecord.getStmtType() != null)
					bsAuditAbt.setInt("STMT_TYPE", auditRecord.getStmtType());
				if (auditRecord.getTransactionId() != null)
					bsAuditAbt.setBytes("TRANSACTION_ID", ByteBuffer.wrap(auditRecord.getTransactionId()));
				if (auditRecord.getReturncode() != null) {
					try {
						bsAuditAbt.setLong("RETURN_CODE", auditRecord.getReturncode().longValueExact());
					} catch (ArithmeticException ae) {
						LOGGER.warn("Can't process Returncode = " + auditRecord.getReturncode());
					}
				}
				if (auditRecord.getScn() != null) {
					try {
						bsAuditAbt.setLong("SCN", auditRecord.getScn().longValueExact());
					} catch (ArithmeticException ae) {
						LOGGER.warn("Can't process SCN = " + auditRecord.getScn());
					}
				}
				if (auditRecord.getCommentText() != null)
					bsAuditAbt.setString("COMMENT_TEXT", auditRecord.getCommentText());
				if (auditRecord.getAuthPrivileges() != null)
					bsAuditAbt.setString("AUTH_PRIVILEGES", auditRecord.getAuthPrivileges());
				if (auditRecord.getOSPrivilege() != null)
					bsAuditAbt.setString("OS_PRIVILEGE", auditRecord.getOSPrivilege());
				if (auditRecord.getGrantee() != null)
					bsAuditAbt.setString("GRANTEE", auditRecord.getGrantee());
				if (auditRecord.getPrivUsed() != null) {
					try {
						bsAuditAbt.setLong("PRIV_USED", auditRecord.getPrivUsed().longValueExact());
					} catch (ArithmeticException ae) {
						LOGGER.warn("Can't process PrivUsed = " + auditRecord.getPrivUsed());
					}
				}
				if (auditRecord.getSesActions() != null)
					bsAuditAbt.setString("SES_ACTIONS", auditRecord.getSesActions());
				if (auditRecord.getObjEditionName() != null)
					bsAuditAbt.setString("OBJ_EDITION_NAME", auditRecord.getObjEditionName());
				if (auditRecord.getPrivGranted() != null) {
					try {
						bsAuditAbt.setLong("PRIV_GRANTED", auditRecord.getPrivGranted().longValueExact());
					} catch (ArithmeticException ae) {
						LOGGER.warn("Can't process PrivGranted = " + auditRecord.getPrivGranted());
					}
				}
				if (auditRecord.getDBID() != null) {
					try {
						bsAuditAbt.setLong("DBID", auditRecord.getDBID().longValueExact());
					} catch (ArithmeticException ae) {
						LOGGER.warn("Can't process DBID = " + auditRecord.getDBID());
					}
				}
				if (auditRecord.getSqlText() != null)
					bsAuditAbt.setString("SQL_TEXT", auditRecord.getSqlText().getValue());
				if (auditRecord.getSqlBind() != null) {
//					if (auditRecord.getSqlBind().isBASE64Encoded()) {
						//TODO decode Base64
						//TODO strange nulls for isBASE64Encoded()!!!!
//					}
					bsAuditAbt.setString("SQL_BIND", auditRecord.getSqlBind().getValue());
				}

				session.execute(bsAuditAbt);

				/* AUDIT_BY_DAY */
				BoundStatement bsAuditByDay = new BoundStatement(insertAuditByDay);
				bsAuditByDay.setUUID("ROWID", rowId);
				bsAuditByDay.setString("DAY", day);
				session.execute(bsAuditByDay);


			}
//			session.execute(insertBatch);
			result = true;
		} catch (Exception e) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
		}
		return result;
	}
}
