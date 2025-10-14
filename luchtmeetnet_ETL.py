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

    # Specify measurements API endpoint, pull JSON response
    url = "https://api.luchtmeetnet.nl/open_api/measurements"
    response = requests.get(url, params=params)
    data = response.json()['data']

    # Indicate whether new data was pulled from the API
    if not data:
        print("No data returned from API.")
        return None
    
    # Create dataframe from JSON response data, push to XCom
    comp_df = pd.json_normalize(data)
    print(f"DATA PULLED:\n{comp_df}\n")
    
    kwargs['ti'].xcom_push(key='comp_data', value=comp_df.to_json(orient='records'))
    print("Data pushed to XCom.")

def load_to_postgres(**kwargs):
    # Initialize Postgres hook and create engine
    hook = PostgresHook(postgres_conn_id="postgres_api_db")
    engine = hook.get_sqlalchemy_engine()

    # Pull comp_df from XCom
    json_data = kwargs['ti'].xcom_pull(task_ids='fetch_api', key='comp_data')
    if not json_data:
        print("No data found in XCom. Skipping insert.")
        return
    
    # Load new data to Postgres database
    comp_df = pd.read_json(json_data)
    print(f"LOADING {len(comp_df)} ROWS TO DATABASE...")

    comp_df.to_sql('component_measurements', engine, if_exists='append', index=False)
    print("Data successfully inserted into Luchtmeetnet database.")

# Define DAG
with DAG(
    dag_id="luchtmeetnet_api_etl",
    description="ETL pipeline for fetching and loading air quality data from the Luchtmeetnet API",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["API", "Postgres", "ETL"],
) as dag:

    # Define fetch task to extract data from API
    fetch_api = PythonOperator(
        task_id="fetch_api",
        python_callable=fetch_api_data,
        doc_md = """
        ### Fetch API Data
        This task requests data from the Luchtmeetnet API on an hourly basis
        and transforms them into a pandas dataframe.
        """,
    )

    # Define loading task to insert data into Postgres database
    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_to_postgres,
        doc_md="""
        ### Load to PostgreSQL
        This task uses a PostgresHook to connect to database and append
        newly extracted data to the component measurements table.
        """,
    )

    # Define task dependencies
    fetch_api >> load_postgres   