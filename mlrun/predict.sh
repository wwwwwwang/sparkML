cd `dirname $0`
echo "\$1=""$1" > parameter_predict.log
sh run.sh yarn-cluster 0 $1
#sh run.sh local $1 $2
cd -
