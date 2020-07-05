from airflow import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator
import pprint as pp
import random
from time import sleep

args = {"owner": "dgnsrekt", "start_date": days_ago(1)}

dag = DAG(dag_id="simple_dag", default_args=args, schedule_interval=None)


def run_this_func(**context):
    recieved_value = context["ti"].xcom_pull(key="random_value")
    print("recieved!", recieved_value)
    pp.pprint(context)


def randomly_fail(**context):
    sleep_time = random.random() * 10
    print("sleeping", sleep_time)
    sleep(sleep_time)
    if random.random() > 0.7:
        raise Exception("EXCEPTION!!!!")
    print("HELLO!")


def push_to_xcom(**context):
    random_value = random.random()
    context["ti"].xcom_push(key="random_value", value=random_value)
    print("complete")


with dag:
    run_this_task = PythonOperator(
        task_id="run_this",
        python_callable=push_to_xcom,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1),
    )

    run_this_task2 = PythonOperator(
        task_id="run_this2", python_callable=run_this_func, provide_context=True
    )

    run_this_task >> run_this_task2
