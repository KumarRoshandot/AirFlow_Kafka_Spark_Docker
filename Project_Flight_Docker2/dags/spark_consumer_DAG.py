import airflow

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

CLIENT = 'kafka:9092'
TOPICS = 'transactions,locations'

checkpoint_trans_path = "/tmp/flight/transaction_checkpoint"
checkpoint_loc_path = "/tmp/flight/location_checkpoint"

args = {
    'owner': 'airflow',
    'description': 'spark Consumer via Docker Operator',
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

task1 = BashOperator(
     task_id='pyspark_consumer',
     bash_command='/usr/spark/bin/spark-submit --master local[*] /spark_consume_data/pyspark_consumer.py {{ params.checkpoint_trans_path }} {{ params.checkpoint_loc_path }} {{ params.CLIENT }} {{ params.TOPICS }}',
     dag=dag,
        )

task1       # set task priority
