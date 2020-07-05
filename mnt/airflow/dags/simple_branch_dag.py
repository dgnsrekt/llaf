from airflow import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import pprint as pp
import random
from time import sleep

args = {"owner": "dgnsrekt", "start_date": days_ago(1)}

dag = DAG(dag_id="simple_branch_dag", default_args=args, schedule_interval=None)


def print_hi(**context):
    recieved_value = context["ti"].xcom_pull(key="random_value")
    print("HI recieved!", recieved_value)
    pp.pprint(context)


def print_hello(**context):
    recieved_value = context["ti"].xcom_pull(key="random_value")
    print("HELLO recieved!", recieved_value)
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


def branch_func(**context):
    if random.random() < 0.5:
        return ("say_hi_task",)
    return "say_hello_task"


with dag:
    run_this_task = PythonOperator(
        task_id="run_this",
        python_callable=push_to_xcom,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1),
    )

    branch_op = BranchPythonOperator(
        task_id="branch_task", provide_context=True, python_callable=branch_func,
    )

    run_this_task2 = PythonOperator(
        task_id="say_hi_task", python_callable=print_hi, provide_context=True
    )

    run_this_task3 = PythonOperator(
        task_id="say_hello_task", python_callable=print_hello, provide_context=True
    )

    run_this_task >> branch_op >> [run_this_task2, run_this_task3]
