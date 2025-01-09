from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract():
    """Fetch data from the weather API"""
    url = "https://api.open-meteo.com/v1/forecast?latitude=35&longitude=139&hourly=temperature_2m"
    response = requests.get(url)
    data = response.json()
    with open('/tmp/weather_data.json', 'w') as f:
        f.write(str(data))

def transform():
    """Transform the raw JSON data"""
    import json
    with open('/tmp/weather_data.json', 'r') as f:
        data = json.load(f)
    transformed_data = [{"timestamp": hour["time"], "temperature": hour["temperature_2m"]} for hour in data["hourly"]["time"]]
    with open('/tmp/transformed_weather_data.json', 'w') as f:
        f.write(json.dumps(transformed_data))

def load():
    """Load the transformed data into PostgreSQL"""
    with open('/tmp/transformed_weather_data.json', 'r') as f:
        data = json.load(f)
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS weather (timestamp TEXT, temperature FLOAT);")
    for record in data:
        cursor.execute("INSERT INTO weather (timestamp, temperature) VALUES (%s, %s)", (record["timestamp"], record["temperature"]))
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

    extract_task >> transform_task >> load_task
