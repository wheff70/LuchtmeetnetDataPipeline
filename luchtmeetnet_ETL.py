from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

def fetch_api_data():
    # Define timezone, declare time variables for API pull parameters
    tz = pytz.timezone('Europe/Amsterdam')
    current_time = datetime.now(tz=tz).strftime('%Y-%m-%dT%H:%M:%S')
    one_hour_ago = (datetime.now(tz=tz) - timedelta(hours=3)).strftime('%Y-%m-%dT%H:%M:%S')

    # Define parameters for API pull
    params = {
        'page': 1,
        'start': f'{one_hour_ago}',
        'end': f'{current_time}',
        'order_by': 'timestamp_measured',
        'order_direction': 'asc'
    }

    # Specify measurements API endpoint, transform JSON response data into dataframe
    url = "https://api.luchtmeetnet.nl/open_api/measurements"
    response = requests.get(url, params=params)
    comp_df = pd.json_normalize(response.json()['data'])
    print(f"DATA PULLED:\n{comp_df}\n")

    # If dataframe is not empty, load component measurements API pull to SQLite table
    if not comp_df.empty:
        print("INSERTING DATA\n")
        '''comp_df.to_sql('component_measurements',
                engine,
                if_exists='append',
                index=False)'''
    else:
        print("comp_df IS EMPTY. SKIPPING INSERT.\n")
        '''print(pd.read_sql("SELECT * FROM component_measurements", engine))'''

with DAG(
    dag_id="luchtmeetnet_api_pull",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["api"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_api",
        python_callable=fetch_api_data,
    )