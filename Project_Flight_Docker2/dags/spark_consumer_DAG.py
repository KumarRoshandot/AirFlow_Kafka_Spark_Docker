import airflow

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

CLIENT = 'kafka:9092'
TOPICS = 'transactions,locations'

checkpoint_trans_path = "/tmp/flight/transaction_checkpoint"
checkpoint_loc_path = "/tmp/flight/location_checkpoint"

args = {
    'owner': 'airflow',
    'description': 'spark Consumer via bash Operator',
    'start_date': airflow.utils.dates.days_ago(1),       # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='spark_consumer_DAG',
    default_args=args,
    schedule_interval='@daily',        # set interval
    catchup=False,                    # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
    params={
        "checkpoint_trans_path": checkpoint_trans_path,
        "checkpoint_loc_path": checkpoint_loc_path,
        "CLIENT": CLIENT,
        "TOPICS": TOPICS
    }
)


task1 = BashOperator(                    # Creating checkpoint Directory if not exists
     task_id='checkpoint_directory_trans',
     bash_command='mkdir -p {{ params.checkpoint_trans_path }}',
     dag=dag,
        )


task2 = BashOperator(                     # Creating checkpoint Directory if not exists
     task_id='checkpoint_directory_loc',
     bash_command='mkdir -p {{ params.checkpoint_loc_path }}',
     dag=dag,
        )

# submitting spark job through shell and passing necessary arguments
task3 = BashOperator(
     task_id='pyspark_consumer',
     bash_command='/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit '
                  '--master local[*] '
                  '--conf "spark.driver.extraClassPath=$SPARK_HOME/jars/kafka-clients-1.1.0.jar" '
                  '--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.1,'
                  'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 '
                  '/usr/local/airflow/dags/src/spark_consume_data/pyspark_consumer.py '
                  '{{ params.checkpoint_trans_path }} '
                  '{{ params.checkpoint_loc_path }} '
                  '{{ params.CLIENT }} '
                  '{{ params.TOPICS }}',
     dag=dag,
        )

task1 >> task2 >> task3       # set task priority
