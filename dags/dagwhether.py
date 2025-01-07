from airfloe import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.provisers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

LATITUDE = '51.5074'
LONGITUDE = '-1278'
POSTGERS_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
    
}

## Define the DAG

with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schesule_interval='@daily',
         catchup=False) as dags:
    
    @task()
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        response = http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'failed to fetch data: {response.text}')
        
    @task()
    def tranform_weather_data(weather_data: dict):
        """Tranform the extracted data."""

        current_wearther = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_wearther['winddirection'],
            'weathercode': current_weather['weathercode']
        }    
        return transformed_data
    
    @task()
    def load_weather_data():
        """Load the transformed data into a Postgres database."""
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
                
            latitude FLOAT, 
            longitude FLOAT, 
            temperature FLOAT, 
            windspeed FLOAT, 
            winddirection FLOAT,  
            weathercode INT
        
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        
        cursor.execute(
            """
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            transformed_data.values()
        )
        
        connection.commit()
        cursor.close()


    weather_data = extract_weather_data()
    transformed_data = tranform_weather_data(weather_data)
    load_weather_data(transformed_data)    