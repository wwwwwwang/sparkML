cd `dirname $0`
#sh run.sh local -p file:///opt/whsh/data/mllib/sample_libsvm_data.txt -s /usr/machine_learning/model/decision_tree_classifier -r 0.7
#sh run.sh local DT_classfication 
#sh run.sh yarn-cluster 14 GM
#sh run.sh local 5 DT_regression
#sh run.sh local 14 GM
#sh run.sh local $1 $2
echo "\$1=""$1"", \$2=""$2" > parameter.log
sh run.sh yarn-cluster $1 $2
#sh run.sh local $1 $2
#sh run.sh yarn-cluster 13 hjw_test_kmean
cd -
