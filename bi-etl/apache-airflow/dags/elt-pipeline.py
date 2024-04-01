import psycopg2
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime
import subprocess
from extract import extractData
from transform import transformData
from load import loadData

# install dependencies
def installDependencies():
    subprocess.run(['pip','install','numpy'])
    subprocess.run(['pip','install','psycopg2'])
    subprocess.run(['pip','install','pandas'])
    subprocess.run(['pip','install','Requests'])

def startPipeline():
    print("start the pipeline")

def endPipeline():
    print("end the pipeline")

# define the DAG
logs_etl_dag = DAG(dag_id='logs-etl',
                         description='An ETL pipeline analysing internet log files',
                         start_date=datetime(2024,4,1))

# define the tasks
start_pipeline = PythonOperator(task_id='start_pipeline',
                       python_callable=startPipeline,
                       dag=logs_etl_dag)
install_dependencies = PythonOperator(task_id='install_dependencies',
                       python_callable=installDependencies,
                       dag=logs_etl_dag)
extract_data = PythonOperator(task_id='extract_data',
                     python_callable=extractData,
                     dag=logs_etl_dag)
transform_data = PythonOperator(task_id='transform_data',
                       python_callable=transformData,
                       dag=logs_etl_dag)
load_data = PythonOperator(task_id='load_data',
                       python_callable=loadData,
                       dag=logs_etl_dag)
end_pipeline = PythonOperator(task_id='end_pipeline',
                       python_callable=endPipeline,
                       dag=logs_etl_dag)

start_pipeline >> install_dependencies >> extract_data >> transform_data >> load_data >> end_pipeline
