from airflow import DAG
from airflow.decorators import task, dag
from pendulum import datetime
from scripts.extract_data import get_data

@dag(
    start_date=datetime(2023,3,27),
    # end_date=datetime(2022,12,1),
    schedule="@daily"
    # catchup=False
)
def get_data_dag():
    @task()
    def execute_get_data():
        get_data()
    
    execute_get_data()
    

get_data_dag()