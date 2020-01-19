import airflow

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'airflow',
    'description': 'spark Consumer via bash Operator in same container',
    'start_date': airflow.utils.dates.days_ago(1),       # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='flights_insight_DAG',
    default_args=args,
    schedule_interval='@daily',        # set interval,@once
    catchup=False,                    # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
)


task1 = BashOperator(
     task_id='data_insight',
     bash_command='/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit '
                  '--master local[*] '
                  '/usr/local/airflow/dags/src/spark_consume_data/data_insights.py',
     dag=dag,
        )

task1
