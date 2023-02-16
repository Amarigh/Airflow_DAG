# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
#defining DAG arguments


# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Amarigh',
    'start_date': days_ago(0),
    'email': ['amarigmustapha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# defining the DAG
# define the DAG
dag = DAG(
    'ETL_server_access_dag',
    default_args=default_args,
    description='ETL server access processing',
    schedule_interval=timedelta(days=1),
)

download= BashOperator(
    task_id='Download',
    bash_command='wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt' ,
    dag=dag,
)

extract= BashOperator(
  task_id='extract',
  bash_command=' cut -d"#" -f1,4 web-server-access-log.txt > extracted-data.txt ',
  dag=dag,
)

transform = BashOperator (
task_id="transform" ,
bash_command=' tr [--lower--] [--upper--] | tr "#" "," > extracted-data.csv',
dag=dag,
)

load = BashOperator(
task_id="load",
bash_command='gzip extracted-dat.csv',
dag=dag,
)

download >> extract >> transform >> load
