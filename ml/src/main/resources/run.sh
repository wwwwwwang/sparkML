#!/bin/sh

basepath=`pwd`
class=`sed "/^class${2}=/!d;s/.*=//" spark_config.cfg`
sparkpath=`sed '/^sparkpath=/!d;s/.*=//' spark_config.cfg`
runjar=`sed '/^runjar=/!d;s/.*=//' spark_config.cfg`
partition=6
master=
case $1 in
  local)
    #id=`sed '/^ID=/!d;s/.*=//' urfile`
    a=`sed '/^lcmaster=/!d;s/.*=//' spark_config.cfg`
    b=`sed '/^lcexecutor-memory=/!d;s/.*=//' spark_config.cfg`
    master="${a} --executor-memory ${b}"
    ;;
  yarn-client)
    master="yarn --deploy-mode client"
    ;;
  yarn-cluster)
    a="yarn --deploy-mode cluster"
    b=`sed '/^yexecutor-memory=/!d;s/.*=//' spark_config.cfg`
    c=`sed '/^yexecutor-cores=/!d;s/.*=//' spark_config.cfg`
    d=`sed '/^ynum-executors=/!d;s/.*=//' spark_config.cfg`
#    partition=$((d>10?d/3:d))
    partition=$((d))
#   e=`sed '/^yspark.default.parallelism=/!d;s/.*=//' spark_config.cfg`
#   f=`sed '/^ydriver-memory=/!d;s/.*=//' spark_config.cfg`
#   g=`sed '/^yspark.memory.fraction=/!d;s/.*=//' spark_config.cfg`
#   h=`sed '/^yspark.memory.storageFraction=/!d;s/.*=//' spark_config.cfg`
#   master="${a} --num-executors ${d} --executor-cores ${c} --executor-memory ${b} --driver-memory ${f} --conf spark.default.parallelism=${e}"
#   master="${a} --num-executors ${d} --executor-cores ${c} --executor-memory ${b} --driver-memory ${f} --conf spark.default.parallelism=${e} --conf spark.memory.fraction=${g} --conf spark.memory.storageFraction=${h}"
    master="${a} --num-executors ${d} --executor-cores ${c} --executor-memory ${b}"
#   master="${a} --executor-cores ${c} --executor-memory ${b}"
#   master="${a} --num-executors ${d} --executor-cores ${c} --executor-memory ${b} --conf spark.memory.fraction=${g} --conf spark.memory.storageFraction=${h}"
    ;;
  standalone)
    a=`sed '/^samaster=/!d;s/.*=//' spark_config.cfg`
    b=`sed '/^executor-memory=/!d;s/.*=//' spark_config.cfg`
    c=`sed '/^executor-cores=/!d;s/.*=//' spark_config.cfg`
    d=`sed '/^num-executors=/!d;s/.*=//' spark_config.cfg`
    e=`sed '/^total-executor-cores=/!d;s/.*=//' spark_config.cfg`
    #master="${a} --executor-cores ${c} --executor-memory ${b} --num-executors ${d}"
    #master="${a} --deploy-mode cluster --executor-cores ${c} --executor-memory ${b} --total-executor-cores ${e}"
    master="${a} --executor-cores ${c} --executor-memory ${b} --total-executor-cores ${e}"
    #master="${a} --executor-cores ${c} --executor-memory ${b}"
    ;;
  *)
    echo "run model is wrong, it should be in loacl|yarn|standalone"
    exit 1
    ;;
esac

function buildClassPath(){
    local jars=
    local SPARK_HOME=$1
    for file in $SPARK_HOME/lib/*.jar
    do
        result=$(echo ${file} | grep "example")
        if [[ "$result" = "" ]]; then
            jars=${jars}${file}:
        fi
    done
    jars=${jars/%:/}
    echo ${jars}
}
function buildJars(){
    local libPath=$1
    local jars=
    for file in ${libPath}/*.jar
    do
        jars=${jars}${file},
    done
    jars=${jars/%,/}
    echo ${jars}
}
function buildFiles(){
    local libPath=$1
    local jars=
    for file in ${libPath}/*
    do
        jars=${jars}${file},
    done
    jars=${jars/%,/}
    echo ${jars}
}
#classpath="${basepath}/config:"
#classpath="$classpath`buildClassPath ${sparkpath}`"
classpath=`buildClassPath ${sparkpath}`
jars=`buildJars ${basepath}/lib`
files=`buildFiles ${basepath}/config`

params=
params="$params --class ${class}"
params="$params --verbose"
params="$params --driver-class-path ${classpath}"
params="$params --files=${files}"
params="$params --jars ${jars}"
params="$params --master ${master}"
command="${sparkpath}/bin/spark-submit ${params}"
#command="$command --conf \"spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps\""
command="$command --conf \"spark.executor.extraJavaOptions=-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+ParallelRefProcEnabled -XX:+CMSClassUnloadingEnabled -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC -XX:+HeapDumpOnOutOfMemoryError -verbose:gc -XX:MaxTenuringThreshold=15 -XX:SurvivorRatio=8\""
#isKafka=$(echo $class | grep "Kafka")
isKafka=$(echo $class | grep "ETL")
if [[ x"$isKafka" != x ]]; then
    max=`sed '/^spark.streaming.kafka.maxRatePerPartition=/!d;s/.*=//' spark_config.cfg`
    blockInterval=`sed '/^spark.streaming.blockInterval=/!d;s/.*=//' spark_config.cfg`
    backPressure=`sed '/^spark.streaming.backpressure.enabled=/!d;s/.*=//' spark_config.cfg`
    #initialRate=`sed '/^spark.streaming.backpressure.initialRate=/!d;s/.*=//' spark_config.cfg`
    command="$command --conf spark.streaming.kafka.maxRatePerPartition=${max}"
    command="$command --conf spark.streaming.blockInterval=${blockInterval}"
    if [[ x"$backPressure" = "xtrue" ]]; then
        command="$command --conf spark.streaming.backpressure.enabled=true"
        #command="$command --conf spark.streaming.backpressure.initialRate=${initialRate}"
    fi
    command="$command $runjar"
    command="$command -p $partition"
else
    command="$command $runjar"
fi
for((i=3;i<=$#;i++)); do
    str="${!i}"
    result=$(echo "${!i}" | grep " ")
    hyphen=$(echo "${!i}" | grep "-")
    if [[ "$result" = "" || "$hyphen" != "" ]]; then
     command="$command $str"
    else
     command="$command \"${str}\""
    fi
done
echo  "command=""$command"
eval "$command"
