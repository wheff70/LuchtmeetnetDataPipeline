from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import pytz

def fetch_api_data(**kwargs):
    # Define timezone, declare time variables for API pull parameters
    tz = pytz.timezone('UTC')
    current_time = datetime.now(tz=tz).strftime('%Y-%m-%dT%H:%M:%S')
    one_hour_ago = (datetime.now(tz=tz) - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')

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
    data = response.json()['data']

    # Indicate whether new data was pulled from the API
    if not data:
        print("No data returned from API.")
        return None
    
    comp_df = pd.json_normalize(data)
    print(f"DATA PULLED:\n{comp_df}\n")
    
    kwargs['ti'].xcom_push(key='comp_data', value=comp_df.to_json(orient='records'))
    print("Data pushed to XCom.")

def load_to_postgres(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_api_db")
    engine = hook.get_sqlalchemy_engine()

    # Pull comp_df from XCom
    json_data = kwargs['ti'].xcom_pull(task_ids='fetch_api', key='comp_data')
    if not json_data:
        print("No data found in XCom. Skipping insert.")
        return
    
    comp_df = pd.read_json(json_data)
    print("LOADING {len(comp_df)} ROWS TO DATABASE...")

    comp_df.to_sql('component_measurements', engine, if_exists='append', index=False)
    print("Data successfully inserted into Luchtmeetnet database.")


with DAG(
    dag_id="luchtmeetnet_api_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["api", "postgres", "etl"],
) as dag:

    fetch_api = PythonOperator(
        task_id="fetch_api",
        python_callable=fetch_api_data
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_to_postgres
    )

    fetch_api >> load_postgres   