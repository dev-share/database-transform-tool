#!/bin/bash
path="${BASH_SOURCE-$0}"
path="$(dirname "${path}")"
path="$(cd "${path}";pwd)"
base=${path}/..
base_path="$(cd "${base}";pwd)"

app_name=database-transform-tool
conf=${base_path}/config/application.properties
log=${base_path}/logs/${app_name}.log
pid=${base_path}/data/${app_name}.pid

if [ -n "${app_name}" ] ; then
	kid=`ps -ef |grep ${app_name}|grep -v grep|awk '{print $2}'`
	echo pid[$kid] from `uname` system process!
fi

if [ -z "$kid" -a -e "$pid" ] ; then
	chmod +x $pid
	kid=`cat $pid`
	echo pid[$kid] from pid file!
fi

if [ -n "${kid}" ]; 
then
	echo ${app_name} pid:${kid}
	kill -9 ${kid}
	echo ----------------------------${app_name} STOPED SUCCESS------------------------------------
else
	echo ${app_name} pid isn't exist or has stoped !
fi

if [ -f $pid ]; then
	rm -rf $pid
	echo "If there is a problem, Please check the log!"
fi