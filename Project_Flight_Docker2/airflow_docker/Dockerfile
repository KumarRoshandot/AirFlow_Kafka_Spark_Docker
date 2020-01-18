FROM rose4earn/spark_airflow:part2

COPY kafka-clients-1.1.0.jar $SPARK_HOME/jars/

WORKDIR /usr/local/airflow/
COPY requirements.txt .
RUN pip install --user -r requirements.txt
