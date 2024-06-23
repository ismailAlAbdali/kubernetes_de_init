from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'datamasterylab',
    'start_date': datetime(2024, 6, 23),
    'catchup': False
}

dag = DAG('hello', default_args=default_args, schedule_interval = timedelta(days=1))

t1 = BashOperator(
    task_id='hello',
    bash_command= 'echo "Hello World!"',
    dag=dag
)
t2 = BashOperator(task_id='hello_dml', bash_command= 'echo "Hello Ismail Data Engineer!"', dag=dag)


t1 >> t2