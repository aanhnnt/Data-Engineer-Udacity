from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'udacity_de_capstone',
    'depends_on_past': False, 
    'start_date': datetime(2022,6,15),
    'retries': 2, 
    'retry_delay': timedelta(minutes=2)
}

with DAG('udacity_de_capstone_project',
    schedule_interval='* 7 * * *', 
    default_args=default_args,
    catchup=False) as dag: 

    etl_task = BashOperator(
    task_id='etl_id',
    bash_command='python /home/workspace/etl.py')

etl_task


  
