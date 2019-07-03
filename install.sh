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
if [[ `id -u` -ne 0 ]] ; then
	 echo "Please run as root" ; exit 1 ;
fi

ORAAUD_HOME=/opt/a2/agents/oraaud

mkdir -p $ORAAUD_HOME/lib
cp target/lib/*.jar $ORAAUD_HOME/lib

cp target/oraaud-kafka-0.9.0.jar $ORAAUD_HOME
cp oraaud-kafka.sh $ORAAUD_HOME
cp oraaud-kafka.conf $ORAAUD_HOME
cp log4j.properties $ORAAUD_HOME

chmod +x $ORAAUD_HOME/oraaud-kafka.sh

