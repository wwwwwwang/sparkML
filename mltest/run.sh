#!/bin/sh

basepath=`pwd`
class=`sed '/^class=/!d;s/.*=//' spark_config.cfg`
sparkpath=`sed '/^sparkpath=/!d;s/.*=//' spark_config.cfg`
runjar=`sed '/^runjar=/!d;s/.*=//' spark_config.cfg`

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
    master="${a} --executor-cores ${c} --executor-memory ${b}"
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
classpath="${basepath}/config:"
classpath="$classpath`buildClassPath ${sparkpath}`"
jars=`buildJars ${basepath}/lib`
#files=`buildFiles ${basepath}/config`

params=
params="$params --class ${class}"
params="$params --verbose"
params="$params --driver-class-path ${classpath}"
params="$params --jars ${jars}"
params="$params --master ${master}"
#params="$params --files=${files}"

echo "${sparkpath}/bin/spark-submit ${params} $runjar $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13}"
${sparkpath}/bin/spark-submit ${params} $runjar "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" "${11}" "${12}" "${13}" 
