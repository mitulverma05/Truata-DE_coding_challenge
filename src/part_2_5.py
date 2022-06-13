#Importing libraries
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


#declaring default args
default_args = {
    'start_date': datetime(2022, 6, 13),
    'owner': 'Airflow'
}


#Initiating dag instance
dag = DAG('my_dag', default_args=default_args, schedule_interval = '@daily', catchup=False)

#Declaring all tasks
task1 = DummyOperator(task_id='task1', dag=dag)
task2 = DummyOperator(task_id='task2', dag=dag)
task3 = DummyOperator(task_id='task3', dag=dag)
task4 = DummyOperator(task_id='task4', dag=dag)
task5 = DummyOperator(task_id='task5', dag=dag)
task6 = DummyOperator(task_id='task6', dag=dag)

task1 >> [task2,task3] >> [task4, task5, task6]