sh run.sh local
#sh run.sh local "select holding_time from jiangyin.entrance_log_time where holding_time < 4000 and no = 10"
#sh run.sh local 2 100 2 "select no,holding_time from jiangyin.entrance_log_time where holding_time < 4000"
#sh run.sh local 4 100 1 "select holding_time from jiangyin.entrance_log_time where holding_time < 4000"
#sh run.sh local 5 100 2 "select no,holding_time from jiangyin.entrance_log_time where holding_time < 4000"
#sh run.sh local 5 10000 1 "select holding_time from jiangyin.entrance_log_time where holding_time < 4000 and no = 10 and holding_time > 0"
#sh run.sh local 50 10000 1 "select holding_time from jiangyin.entrance_log_time where holding_time < 4000 and no = 10 and holding_time > 0"
#sh run.sh local 5 10000 1 /usr/machine_learning/model/k_means "select holding_time from jiangyin.entrance_log_time where holding_time < 4000 and no = 10 and holding_time > 0"
#sh run.sh local /usr/machine_learning/model/k_means 288
#sh run.sh local /usr/machine_learning/model/k_means 1 "select holding_time from jiangyin.k_means_predict_test"
