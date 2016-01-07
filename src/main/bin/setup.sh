#!/bin/sh
hadoop fs -put -f inject /
hadoop fs -mkdir /nutch
hadoop fs -put -f lib /nutch
hadoop fs -put -f plugin /
