#!/bin/bash

tmp_config=`mktemp`

echo benchmark.class=$1 > $tmp_config
echo benchmark.properties=$2 >> $tmp_config

echo client.class=$3 >> $tmp_config
echo client.properties=$4 >> $tmp_config

echo $tmp_config content:
#cat $tmp_config

java -cp target/nosql-benchmarking-framework-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.github.bluetiger9.nosql.benchmarking.Main $tmp_config
