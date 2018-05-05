from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import  DummyOperator


def hello_world(ds, **kwargs):
    return "Hello Words at {!r} {!s}".format(ds,ds)


dag = DAG("HelloWordApp", description="First attempt to handle Airflow", schedule_interval="0 22 * * *",
          start_date=datetime(2018,5,1))

with dag:
    write_hello = PythonOperator(task_id='write_helo', python_callable=hello_world,provide_context=True)
    do_nothing = DummyOperator(task_id='dummy')
    write_hello >> do_nothing