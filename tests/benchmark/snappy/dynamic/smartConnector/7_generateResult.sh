#!/usr/bin/env bash
source PerfRun.conf

directory=$outputLocation/GeneratedResults/$(date "+%Y.%m.%d-%H.%M.%S")_$dataSize
mkdir -p $directory
mkdir -p $directory/conf/

cp PerfRun.conf $directory/
cp $SPARK_HOME/conf/spark-env.sh $SPARK_HOME/conf/spark-defaults.conf $directory/
mv *.csv *.out $directory/

cd $SnappyData/conf
cp leads locators servers *.properties $directory/conf/

latestProp=$directory/latestProp.props

cd $SNAPPY_HOME
cd ../../../
echo snappyData = $(git rev-parse HEAD)_$(git log -1 --format=%cd) > $latestProp
cd spark
echo snappy-spark = $(git rev-parse HEAD)_$(git log -1 --format=%cd) >> $latestProp


echo "******************Performance Result Generated*****************"

