import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# TEST
args = {
    "owner": "dgnsrekt",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
}

dag = DAG(dag_id="my_first_dag", default_args=args, schedule_interval="0 2 * * *",)


def print_hello():
    return "hellowa!"


def print_goodbye():
    return "gooda bye a"


with dag:
    task_one = PythonOperator(task_id="print_hello", python_callable=print_hello,)
    task_two = PythonOperator(task_id="print_goodbye", python_callable=print_goodbye,)

    task_one >> task_two
