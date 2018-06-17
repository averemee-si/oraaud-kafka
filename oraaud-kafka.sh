#!/bin/sh
#
# Copyright (c) 2018-present, http://a2-solutions.eu
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
# the License for the specific language governing permissions and limitations under the License.
#

if type -p java; then
    JAVA=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    JAVA="$JAVA_HOME/bin/java"
else
    echo "DID NOT FIND JAVA. EXITING!!!"
    exit 1
fi

A2_AGENT_HOME=/opt/a2/agents/oraaud

nohup $JAVA \
    -cp $(for i in $A2_AGENT_HOME/lib/*.jar ; do echo -n $i: ; done)$A2_AGENT_HOME/oraaud-kafka-0.1.0.jar \
    -Da2.log4j.configuration=$A2_AGENT_HOME/log4j.properties eu.solutions.a2.audit.ora112.OraaudKafka \
    $A2_AGENT_HOME/oraaud-kafka.conf </dev/null 2>&1 | tee oraaud-kafka.log &
