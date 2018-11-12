cd `dirname $0`
echo "\$1=""$1"", \$2=""$2" > parameter_train.log
sh run.sh yarn-cluster $1 $2
#sh run.sh local $1 $2
cd -
