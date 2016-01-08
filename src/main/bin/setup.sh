#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIR}/config.sh

hadoop fs -rm -r -f /inject
hadoop fs -put -f ${NUTCH_DIR}/inject /

hadoop fs -rm -r -f /nutch
hadoop fs -mkdir /nutch
hadoop fs -put -f ${NUTCH_DIR}/lib /nutch
hadoop fs -put -f ${NUTCH_DIR}/plugin /
hadoop fs -rm /nutch/lib/nutch-2.2.jar
