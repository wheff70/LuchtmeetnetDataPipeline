from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import pytz

default_args = {
    'owner': 'william_heffernan',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(minutes=10),
    'depends_on_past': True
}

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

def validate_data(**kwargs):
    json_data = kwargs['ti'].xcom_pull(task='fetch_api', key='comp_data')
    if not json_data:
        print("No data found in XCom for validation. Skipping.")
        return None

    df = pd.read_json(json_data)
    hook = PostgresHook(postgres_conn_id="postgres_api_db")
    engine = hook.get_sqlalchemy_engine()
    

    # Initialize metrics
    total_rows = len(df)
    tz = pytz.timezone('UTC')
    
    metrics = {
        "timestamp": datetime.now(tz=tz).strftime('%Y-%m-%dT%H:%M:%S'),
        "total_rows": total_rows,
        "duplicates_removed": 0,
        "nulls_dropped": 0,
        "negatives_corrected": 0,
        "rows_quarantined": 0
    }

    # Validation rules
    print(f"Starting data validation and cleaning: {total_rows} rows received")

    # Check required columns
    required_columns = ['station_number', 'value', 'timestamp_measured', 'formula']
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Drop duplicates
    before = len(df)
    df.drop_duplicates(inplace=True)
    after = len(df)
    if before != after:
        print(f"Removed {before - after} duplicate rows.")

    # Handle null values
    null_rows = df[df['value'].isnull()]
    if not null_rows.empty:
        null_rows.to_sql('invalid_measurements', engine, if_exists='append', index=False)
        metrics['nulls_dropped'] = len(null_rows)
        metrics['rows_quarantined'] += len(null_rows)
        df = df.dropna(subset=['value'])

    # Handle negative values
    neg_rows = df[df['value'] < 0]
    if not neg_rows.empty:
        neg_rows.to_sql('invalid_measurements', engine, if_exists='append', index=False)
        metrics['nulls_dropped'] = len(neg_rows)
        metrics['rows_quarantined'] += len(neg_rows)
        df = df[df['value'] >= 0]

    # Log quality metrics
    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_sql('data_validation_log', engine, if_exists='append', index=False)
    print(metrics_df)

    # Push cleaned data
    kwargs['ti'].xcom_push(key='clean_data', value=df.to_json(orient='records'))
    print(f"Data validation & cleaning complete. {len(df)} rows ready for loading.")

def load_to_postgres(**kwargs):
    # Initialize Postgres hook and create engine
    hook = PostgresHook(postgres_conn_id="postgres_api_db")
    engine = hook.get_sqlalchemy_engine()

    # Pull comp_df from XCom
    json_data = kwargs['ti'].xcom_pull(task_ids='validate_data', key='validated_data')
    if not json_data:
        print("No validated data found in XCom. Skipping insert.")
        return
    
    # Load new data to Postgres database
    comp_df = pd.read_json(json_data)
    print(f"Loading {len(comp_df)} rows to database...")

    comp_df.to_sql('component_measurements', engine, if_exists='append', index=False)
    print("Data successfully inserted into Luchtmeetnet database.")

# Define DAG
with DAG(
    dag_id="luchtmeetnet_api_etl",
    description="ETL pipeline for fetching and loading air quality data from the Luchtmeetnet API",
    default_args=default_args,
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
        Requests data from the Luchtmeetnet API on an hourly basis
        and transforms them into a pandas dataframe.
        """,
    )

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        doc_md="""
        ### Validate Extracted Data
        Converts data pulled from the API to a Pandas dataframe and
        checks schema, nulls, ranges, and duplicates before loading
        to PostgreSQL.
        """
    )

    # Define loading task to insert data into Postgres database
    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_to_postgres,
        doc_md="""
        ### Load to PostgreSQL
        Utilizes a PostgresHook to connect to database and append
        newly extracted data to the component measurements table.
        """,
    )

    # Task dependencies
    fetch_api >> validate_data >> load_postgres   