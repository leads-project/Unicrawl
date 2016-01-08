#!/bin/sh
export HADOOP_DIST_HOME=
export NUTCH_DIR=
export HDFS_NAMENODE=localhost

export HADOOP_COMMON_HOME=${HADOOP_DIST_HOME}
export HADOOP_CONF_DIR=${HADOOP_DIST_HOME}/etc/hadoop
export HADOOP_HDFS_HOME=${HADOOP_DIST_HOME}
export HADOOP_MAPRED_HOME=${HADOOP_DIST_HOME}
export HADOOP_YARN_HOME=${HADOOP_DIST_HOME}
export PATH=$HADOOP_DIST_HOME/bin:$HADOOP_DIST_HOME/sbin:$PATH

