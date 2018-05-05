from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import  DummyOperator
import logging

def hello_world(ds, **kwargs):
    logging.info("Hello Words v2 at {!r} {!s}".format(ds,ds))


def write_1(call_id, **kwargs):
    logging.info("Call id {id} on {date}".format(id=call_id,date=kwargs['ds']))

dag = DAG("HelloWordApp_v2", description="First attempt to handle Airflow", schedule_interval="@once",
          start_date=datetime(2018,5,1))

with dag:
    write_hello = PythonOperator(task_id='write_helo', python_callable=hello_world,provide_context=True)
    for i in range(3):
        do_nothing = PythonOperator(task_id='dummy_{}'.format(i),python_callable=write_1, provide_context=True,
                                    op_kwargs={'call_id': i} )
        write_hello >> do_nothing