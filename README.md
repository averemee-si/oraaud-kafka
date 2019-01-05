# oraaud-kafka

[Oracle Database](https://www.oracle.com/database/index.html) audit files to [Apache Kafka](http://kafka.apache.org/) transfer.

## Getting Started

These instructions will get you a copy of the project up and running on your Oracle Database Server (at moment only Linux/Solaris/AIX OS are supported).

### Prerequisites

Before using oraaud-kafka please check that required Java8+ is installed with

```
echo "Checking Java version"
java -version
```
and following RDBMS parameters are set correctly

```
sqlplus / as sysdba
select NAME, VALUE from v$parameter where NAME in ('audit_sys_operations', 'audit_file_dest','audit_trail');
REM or use 'show parameter' command
```
Is it recommended to set **audit_file_dest** to separate filesystem/LUN/etc from RDBMS datafiles, tempfiles and logfiles.

We recommend to set **audit_sys_operations** to  **TRUE**.

For working with **oraaud-kafka** Oracle RDBMS parameter **audit_trail** must be set to **XML, EXTENDED**


### Installing

Build with

```
mvn install
```
Then run as root supplied `install.sh` or run commands below

```
ORAAUD_HOME=/opt/a2/agents/oraaud

mkdir -p $ORAAUD_HOME/lib
cp target/lib/*.jar $ORAAUD_HOME/lib

cp target/oraaud-kafka-0.1.0.jar $ORAAUD_HOME
cp oraaud-kafka.sh $ORAAUD_HOME
cp oraaud-kafka.conf $ORAAUD_HOME
cp log4j.properties $ORAAUD_HOME

chmod +x $ORAAUD_HOME/oraaud-kafka.sh
```



Create Kafka's server topic for audit test with a single partition and only one replica: 

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic oraaud-test
```

Edit `oraaud-kafka.conf`, this files should looks like

```
a2.watched.path = /data/oracle/adump
a2.worker.count = 32
a2.locked.file.query.interval = 512
a2.kafka.servers = dwh.a2-solutions.eu:9092
a2.kafka.topic = ora-audit-topic
a2.kafka.client.id = a2.audit.ai.ora112

```
`a2.watched.path` - valid directory path, must match Oracle RDBMS parameter **audit_file_dest**

`a2.worker.count` - number of threads for transferring audit information to Kafka cluster

`a2.locked.file.query.interval` - interval in milliseconds between check for "file in use"

 
`a2.kafka.servers` - hostname/IP address and port of Kafka installation

`a2.kafka.topic` - value must match name of Kafka topic created on previous step

`a2.kafka.client.id` - use any valid string value for identifying this Kafka producer

`a2.security.protocol` - `SSL` if you like to transmit files using SSL

`a2.security.truststore.location` - set to valid certificate store file if `a2.security.protocol` set to `SSL`

`a2.security.truststore.password` - password for certificate store file if `a2.security.protocol` set to `SSL`


## Running 

Use FGA


```
begin
 DBMS_FGA.ADD_POLICY(
	object_schema => 'AP',
	object_name => 'AP_INVOICES_ALL',
	policy_name => 'AP_INV_AUD',
    enable => TRUE,
	statement_types => 'INSERT,UPDATE,SELECT',
	audit_trail => DBMS_FGA.XML + DBMS_FGA.EXTENDED);
End;
/
```
or audit command 

```
AUDIT SELECT
  ON per.per_all_people_f;
```
For other examples please consult Oracle Database documentation. Check for audit information at [Kafka](http://kafka.apache.org/)'s side with command line consumer

```
bin/kafka-console-consumer.sh --from-beginning --zookeeper localhost:2181 --topic oraaud-test
```

## Deployment

Do not forget to align [Kafka](http://kafka.apache.org/)'s message.max.bytes and replica.fetch.max.bytes parameters with Oracle Database audit file size. To check current values in Oracle database use

```
column PARAMETER_NAME format A30
column PARAMETER_VALUE format A20
column AUDIT_TRAIL format A20
select PARAMETER_NAME, PARAMETER_VALUE, AUDIT_TRAIL
from DBA_AUDIT_MGMT_CONFIG_PARAMS
where PARAMETER_NAME like '%SIZE%'
```
To change size of audit xml file

```
BEGIN
  DBMS_AUDIT_MGMT.set_audit_trail_property(
    audit_trail_type           => DBMS_AUDIT_MGMT.AUDIT_TRAIL_XML,
    audit_trail_property       => DBMS_AUDIT_MGMT.OS_FILE_MAX_SIZE,
    audit_trail_property_value => 1000);
END;
/

BEGIN
  DBMS_AUDIT_MGMT.set_audit_trail_property(
    audit_trail_type           => DBMS_AUDIT_MGMT.AUDIT_TRAIL_UNIFIED,
    audit_trail_property       => DBMS_AUDIT_MGMT.OS_FILE_MAX_SIZE,
    audit_trail_property_value => 1000);
END;
/

BEGIN
  DBMS_AUDIT_MGMT.set_audit_trail_property(
    audit_trail_type           => DBMS_AUDIT_MGMT.AUDIT_TRAIL_OS,
    audit_trail_property       => DBMS_AUDIT_MGMT.OS_FILE_MAX_SIZE,
    audit_trail_property_value => 1000);
END;
/
```
There is comparision of [Apache Kafka](http://kafka.apache.org/) performance and throughtput for different message size - [Finding Kafka optimal message size](https://www.idata.co.il/2018/02/finding-kafka-optimal-message-size/) and we recommend not to set Oracle Database audit file size higher than 1M.


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## TODO
* Kerberos support
* Windows

## Authors

* **Aleksej Veremeev** - *Initial work* - [A2 Solutions](http://a2-solutions.eu/)

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

