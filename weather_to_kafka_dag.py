from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_script():
    subprocess.run(["python3", "D:\Muthu\api\weather_to_kafka.py"])

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "weather_to_kafka_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",  # Every minute
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="send_weather_data_to_kafka",
        python_callable=run_script
    )
