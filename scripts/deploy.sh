export RUN_CLASS=com.example.Assignment1
export MASTER_HOSTNAME=local
export JAR_PATH=/users/agabhin/Assignment1.jar

./bin/spark-submit --class "${RUN_CLASS}" --master ${MASTER_HOSTNAME} ${JAR_PATH}