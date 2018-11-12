#sh run.sh local -p file:///opt/whsh/data/mllib/sample_libsvm_data.txt -s /usr/machine_learning/model/decision_tree_classifier -r 0.7
#sh run.sh local -p file:///opt/whsh/data/whsh/datasets/glass.txt -z -n 6 -i 100 -d
#sh run.sh local -p file:///opt/whsh/data/whsh/datasets/wine.txt -z -d 
#sh run.sh local -p file:///opt/whsh/data/whsh/datasets/iris.txt -s /usr/machine_learning/model/k_mean_test -z 
sh run.sh local -q "select cluster_no, sepal_length, sepal_width, petal_length, petal_width from jiangyin.iris" -z 
#sh run.sh local -p file:///opt/whsh/data/whsh/datasets/2d4c.txt -s /usr/machine_learning/model/k_mean_test -z -n 4 -i 100
#sh run.sh local DT_classfication 
