from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from pathlib import Path
import json
import requests

ROOT = Path(__file__).parent

forex_file = ROOT / "files" / "forex_currencies.json"
outfile_path = ROOT / "files" / "forex_rates.json"


def download_rates():
    with open(forex_file, mode="r") as file:
        data = json.loads(file.read())

    for base, pairs in data.items():
        indata = requests.get(f"https://api.exchangeratesapi.io/latest?base={base}").json()
        outdata = {"base": base, "rates": {}, "last_update": indata["date"]}
        for pair in pairs:
            outdata["rates"][pair] = indata["rates"][pair]

        with open(outfile_path, "a") as outfile:
            json.dump(outdata, outfile)
            outfile.write("\n")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 7, 2),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@host.com",
    "retires": 1,
    "retries_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="forex_data_pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
)

# NOTE try this out
def is_forex_rates_available_response_check(response):
    return "rates" in response.text


with dag:
    is_forex_rates_avaiable = HttpSensor(
        task_id="is_forex_rates_available",
        method="GET",
        http_conn_id="forex_api",
        endpoint="latest",
        # response_check=lambda response: "rates" in response.text,
        response_check=is_forex_rates_available_response_check,
        poke_interval=5,
        timeout=20,
    )

    is_forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        filepath="forex_currencies.json",
        fs_conn_id="forex_path",
        poke_interval=5,
        timeout=20,
    )

    download_rates = PythonOperator(task_id="download_rates", python_callable=download_rates)

    saving_rates = BashOperator(
        task_id="saving_rates",
        bash_command="""
        hdfs dfs -mkdir -p /forex && \
        hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
    """,
    )

    is_forex_rates_avaiable >> is_forex_currencies_file_available >> download_rates >> saving_rates
