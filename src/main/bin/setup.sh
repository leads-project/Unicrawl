#!/bin/bash
export NUTCHDIR="/home/ubuntu/nutch"
source ${NUTCHDIR}/bin/config.sh

hadoop fs -rm -r -f /inject
hadoop fs -put -f ${NUTCHDIR}/inject /

hadoop fs -rm -r -f /nutch
hadoop fs -mkdir /nutch
hadoop fs -put -f ${NUTCHDIR}/lib /nutch
hadoop fs -put -f ${NUTCHDIR}/plugin /
hadoop fs -rm /nutch/lib/nutch-2.2.jar
