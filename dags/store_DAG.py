from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator

from datacleaner import data_cleaner

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("store_dag", default_args=default_args, schedule_interval='@daily', template_searchpath=['/usr/local/airflow/sql_files'], catchup=False)

t1 = BashOperator(task_id="check_datafile_exists", bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15), dag=dag)

t2 = PythonOperator(task_id="clean_raw_csv", python_callable=data_cleaner, dag=dag)

#first, create connection 
t3 = MySqlOperator(task_id="create_mysql_table", mysql_conn_id="mysql_conn", sql="create_table.sql", dag=dag)

t4 = MySqlOperator(task_id="insert_into_table", mysql_conn_id="mysql_conn", sql="insert_into_table.sql", dag=dag)

#use bit operators instead of set_upstream & set_downstream
t1 >> t2 >> t3 >> t4