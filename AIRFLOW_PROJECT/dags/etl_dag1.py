#import libraries

from airflow.decorators import dag, task
from datetime import datetime
import packages.functions  


#create dag and tasks

@dag(schedule_interval=None, start_date=datetime(2025, 9, 28), catchup=False)
def weather_etl_dag():
    @task()
    def extract_task():
        return packages.functions.extract_weather_data()

    @task()
    def transform_task(data):
        return packages.functions.transform_weather_data(data)

    @task()
    def load_task(transformed_data):
        return packages.functions.load_weather_data(transformed_data)

    #define DAG flow

    extracted_data = extract_task()
    transformed_data = transform_task(extracted_data)
    load_task(transformed_data)

weather_etl_dag()
