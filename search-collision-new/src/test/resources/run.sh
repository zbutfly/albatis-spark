#!/usr/bin/env bash
i#!/bin/bash
set -o nounset
set -o errexit
export LANG=zh_CN.UTF-8

JAVA_HOME="/usr/java/jdk1.8.0_111"
PATH=${JAVA_HOME}/bin:${PATH}
java -version

JAVA_OPTS=
#JAVA_OPTS="${JAVA_OPTS} -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=49901"
JAVA_OPTS="${JAVA_OPTS} -Dbus.threadpool.size=300 -Dbus.server.context=search-collision-new -Dbus.server.base=/opt/cominfo/test/tempfile/"
JAVA_OPTS="${JAVA_OPTS} -DDEBUG.MONGO=true -DDB.TRACE=true"

JAVA_OPTS="${JAVA_OPTS} -Xms2g -Xmx2g "
JAVA_OPTS="${JAVA_OPTS} -XX:+UseConcMarkSweepGC"
echo JAVA_OPTS:	${JAVA_OPTS}

#use new log4j
DAG_CP="./test-classes:./$1.jar:./dependency/log4j-1.2.17.jar:./dependency/*"
echo CLASSPATH:	${DAG_CP}

DAG_CMD="java ${JAVA_OPTS} -cp ${DAG_CP} -Dbus.port=$2 net.butfly.bus.start.JettyStarter $1-server.xml"
echo CMD:		${DAG_CMD}
${DAG_CMD}
