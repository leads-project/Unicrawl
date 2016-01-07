#!/bin/bash

export NUTCHDIR="/opt/nutch"
source ${NUTCHDIR}/bin/config.sh
uclouds=(leads-qe3 leads-qe8 leads-qe28)

function stopExp(){
    pkill -TERM $(ps aux | grep 'unicrawl.sh' | grep -v grep | awk '{print $2}') 2&>1 > /dev/null
    exit 0
}



for ucloud in ${uclouds[@]}
do
    ssh ${ucloud} "cd ${NUTCHDIR} && ./bin/dnutch $1" > ${NUTCHDIR}/ucloud_${ucloud}.txt &
done;
wait

trap "stopExp; wait; exit 255" SIGINT SIGTERM

exit 0
