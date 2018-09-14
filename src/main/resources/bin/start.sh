#!/bin/bash
path="${BASH_SOURCE-$0}"
path="$(dirname "${path}")"
path="$(cd "${path}";pwd)"
base=${path}/..
base_path="$(cd "${base}";pwd)"

app_name=database-transform-tool
conf=${base_path}/config/config.properties
log=${base_path}/logs/${app_name}.log
pid=${base_path}/data/${app_name}.pid
if [ -f $pid ] ; then
	echo "please run stop.sh first,then start.sh" 2>$2
	exit 1;
fi

if [ -n "${app_name}" ] ; then
	kid = `ps -ef |grep ${app_name}|grep -v grep|awk '{print $2}'`
	echo pid[$kid] from `uname` system process!
fi

if [ -n $kid ] ; then
	echo [`uname`] ${app_name} process [$kid] is Running!
	exit 1;
fi

if [ -f $log ] ; then
	rm -rf ${base_path}/logs/*
	rm -rf $pid
fi

if [ ! -d ${base_path}/logs ] ; then
	mkdir -p ${base_path}/logs
fi

if [ ! -d ${base_path}/data ] ; then
	mkdir -p ${base_path}/data
fi

if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi
JAVA_ENV="-server -Xms2g -Xmx2g -Xss1m "
JAVA_OPTS="$JAVA_ENV -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:+AlwaysPreTouch -Djava.awt.headless=true -Dfile.encoding=UTF-8 -Djna.nosys=true -Djdk.io.permissionsUseCanonicalPath=true -Dio.netty.noUnsafe=true -Dio.netty.noKeySetOptimization=true -Dio.netty.recycler.maxCapacityPerThread=0 -Dlog4j.shutdownHookEnabled=false -Dlog4j2.disable.jmx=true -Dlog4j.skipJansi=true -XX:+HeapDumpOnOutOfMemoryError "

for i in "${base_path}"/lib/*.jar
do
    CLASSPATH="$i:$CLASSPATH"
done

if [ -e $conf -a -d ${base_path}/logs ]
then
	echo -------------------------------------------------------------------------------------------
	cd ${base_path}
	
	for file in "${base_path}"/*.jar
	do
	    file=${file##*/}
	    filename=${file%.*}
	    echo -----------------file=${file},filename=${filename}------------------
	    if [[ $filename =~ $app_name ]]; then
	    	app=$file
	    	echo app jar:$app
	    	break;
	    fi
	done
	
	echo ${app_name} Starting ...
	$JAVA $JAVA_OPTS -classpath=.:$CLASSPATH -cp $app:"${base_path}"/lib/*.jar com.ucloudlink.css.Application -spring.config.location=$conf -base.path=${base_path} >$log 2>&1 &
	echo $! > $pid
	
	kid = `ps -ef |grep ${app_name}|grep -v grep|awk '{print $2}'`
	if [ -n $kid ] ; then
		echo ----------------------------${app_name} STARTED SUCCESS------------------------------------
	else
		echo ----------------------------${app_name} STARTED ERROR------------------------------------
	fi
	echo -------------------------------------------------------------------------------------------
else
	echo "${app_name} config($conf) Or logs direction is not exist,please create first!"
	rm -rf $pid
fi