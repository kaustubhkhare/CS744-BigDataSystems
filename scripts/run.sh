MASTER_HOSTNAME=spark://$1:7077
JAR_PATH=$2
RUN_CLASS=com.example.$3
NUM_EXEC=2
EXEC_CORES=5
EXEC_MEMORY=30G
DRIVER_MEMORY=30G

shift 3

if [[ $# -eq 0 ]]; then
	a1="hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/"
	a2="hdfs://10.10.1.1:9000/data/task2/output/"
	read -p  "use default vals [ctrl-c] to cancel? $a1 $a2" -s _
else
	a1=$1
	a2=$2
fi

time -p $(find . -name spark-submit) --class "${RUN_CLASS}" --master ${MASTER_HOSTNAME} \
#	--num-executors ${NUM_EXEC} \
	--executor-cores ${EXEC_CORES} \
	--executor-memory ${EXEC_MEMORY} \
	--driver-memory ${DRIVER_MEMORY} \
	--driver-cores 1 \
	${JAR_PATH} $a1 $a2
md5sum $JAR_PATH
ls -l $JAR_PATH