#!/bin/bash
export NUTCHDIR="/home/ubuntu/nutch"
source ${NUTCHDIR}/bin/config.sh

uclouds=(80.156.73.93 5.147.254.197)

for ucloud in ${uclouds[@]}
do
    ssh ${ucloud} "cd ${NUTCHDIR} && ./bin/dnutch $1" > ${NUTCHDIR}/ucloud_${ucloud}.txt &
done;
wait
