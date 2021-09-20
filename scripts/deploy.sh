RUN_CLASS=com.example.Assignment1
MASTER_HOSTNAME=192.168.0.105
MASTER_PORT=8080
JAR_PATH=/Users/kaustubh/Development/SparkProjects/CS744-BigDataSystems-Assignment1/target/scala-2.12/Assignment1.jar

./bin/spark-submit --class ${RUN_CLASS} --master spark://${MASTER_HOSTNAME}:${MASTER_PORT} ${JAR_PATH}