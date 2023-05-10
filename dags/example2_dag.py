import airflow
from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
import requests
from scripts.extract_data import get_prices
import pandas as pd

from io import StringIO

from pendulum import datetime,now
S3BUCKET_NAME = 'lenistest'

def etl_alkosto_function():
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    urls = s3_hook.read_key(
        key='data_alkosto/urls_alkosto.csv', bucket_name=S3BUCKET_NAME
    )
    old_data = s3_hook.read_key(
        key='data_alkosto/out_alkosto.csv',bucket_name=S3BUCKET_NAME
    )
    df_old_data = pd.read_csv(StringIO(old_data))
    df_urls = pd.read_csv(StringIO(urls))
    
    df_result = get_prices(df_urls)
    
    df_old_data = pd.concat([df_old_data,df_result])
    new_data = df_old_data.to_csv(index=False)
    s3_hook.load_string(new_data,'data_alkosto/out_alkosto.csv',S3BUCKET_NAME,replace=True)


    response = {"message":"OK"}
    print('Response',response)


@dag(
    start_date=datetime(2023,4,15),
    schedule="@daily",
    default_args={"depends_on_past":True},
    max_active_runs=1,
    catchup=False,
)
def example_2_dag():

    etl_alkosto = PythonOperator(
        task_id="etl_alkost", python_callable=etl_alkosto_function
    )
    etl_alkosto

example_2_dag()