FROM rose4earn/spark_airflow_hive:part1

# This is Require to solve the partition.assignment.strategy during spark-submit
COPY kafka-clients-1.1.0.jar $SPARK_HOME/jars/

# These are required , as we need hive and spark connection
COPY hive-site.xml $HIVE_HOME/conf
COPY hive-site.xml $SPARK_HOME/conf
COPY hive $HIVE_HOME/bin
COPY spark-hive_2.11-2.3.1.jar $HADOOP_HOME/share/hadoop/common/lib

WORKDIR /usr/local/airflow/
COPY requirements.txt .
RUN pip install --user -r requirements.txt
