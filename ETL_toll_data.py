
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
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# defining the DAG
# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

download= BashOperator(
    task_id='Download',
    bash_command='wget  https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz' ,
    dag=dag,
)

unzip_data= BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -xvf tolldata.tar',
    dag=dag,
)

extract_data_from_csv= BashOperator(
  task_id='extract_data_from_csv',
  bash_command=" sudo cut -d',' -f1-4  vehicle-data.csv > csv_data.csv ",
  dag=dag,
)


extract_data_from_tsv= BashOperator(
  task_id='extract_data_from_tsv',
  bash_command=" sudo cut -d$'\t' -f5-7  tollplaza-data.tsv  | tr '\t' ',' > tsv_data.csv ",
  dag=dag,
)


extract_data_from_fixed_width= BashOperator(
  task_id='extract_data_from_fixed_width',
  bash_command=" sudo cut -c59-68 payment-data.txt  | tr ' ' ',' > fixed_width_data.csv ",
  dag=dag,
)

consolidate_data= BashOperator(
  task_id='consolidate_data',
  bash_command=" sudo paste csv_data.csv tsv_data.csv fixed_width_data.csv | tr '\t' ','  > extracted_data.csv ",
  dag=dag,
)

transform_data= BashOperator(
  task_id='transform_data',
  bash_command=" sudo sed 's/car/CAR/g ; s/van/Van/g; s/truck/TRUCK/g'  extracted_data.csv  > transformed_data.csv ",
  dag=dag,
)



download >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

