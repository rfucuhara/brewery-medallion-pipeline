from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.email import send_email
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import requests
import json
import math
import logging
from psycopg2.extras import execute_values
from airflow.configuration import conf

BRONZE_BREWERIES_DATASET = Dataset("postgres://supabase/bronze/breweries")

def notify_failure(context):
    ti = context['task_instance']
    subject = f"🚨 Fail in Bronze Pipeline: {ti.dag_id}"
    receiver_email = conf.get('smtp', 'smtp_user')
    body = f"Task Error {ti.task_id}. See details: {ti.log_url}"
    send_email(to=receiver_email, subject=subject, html_content=body)

default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'on_failure_callback': notify_failure, 
    'execution_timeout': timedelta(minutes=30),
}

@dag(
    dag_id='brewery_ingestion_bronze',
    default_args=default_args,
    schedule_interval='0 9 * * *', 
    start_date=datetime(2026, 2, 22),
    catchup=False,
    tags=['bees', 'bulk_upsert', 'performance', 'real_incremental'],
)

def brewery_bronze_pipeline():

    @task
    def health_check_supabase():
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        pg_hook.get_first("SELECT 1")
        logging.info("Supabase Online.")

    @task
    def setup_database():
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        pg_hook.run("CREATE SCHEMA IF NOT EXISTS bronze;")
        pg_hook.run("""
            CREATE TABLE IF NOT EXISTS bronze.breweries (
                id_api TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

    @task
    def get_initial_state():
        """Captures the state timestamp before loading for delta comparison"""
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        res = pg_hook.get_first("SELECT MAX(updated_at) FROM bronze.breweries")
        return res[0].isoformat() if res and res[0] else None

    @task
    def get_pages_to_process():
        meta_url = "https://api.openbrewerydb.org/v1/breweries/meta"
        response = requests.get(meta_url, timeout=15)
        response.raise_for_status()
        total_records = int(response.json()['total'])
        return list(range(1, math.ceil(total_records / 200) + 1))

    @task(max_active_tis_per_dag=10)
    def fetch_and_validate_to_bronze(page):
        url = "https://api.openbrewerydb.org/v1/breweries"
        response = requests.get(url, params={'page': page, 'per_page': 200}, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data: return 0

        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        validated_rows = []
        now = datetime.now() 

        for item in data:
            if all(k in item for k in ['id', 'name', 'brewery_type', 'city', 'country']):
                validated_rows.append((item['id'], json.dumps(item), now))

        if validated_rows:
            upsert_sql = """
                INSERT INTO bronze.breweries (id_api, payload, updated_at)
                VALUES %s
                ON CONFLICT (id_api) 
                DO UPDATE SET 
                    payload = EXCLUDED.payload,
                    updated_at = CASE 
                        WHEN breweries.payload IS DISTINCT FROM EXCLUDED.payload THEN EXCLUDED.updated_at
                        ELSE breweries.updated_at
                    END
                WHERE breweries.payload IS DISTINCT FROM EXCLUDED.payload;
            """
            
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            try:
                execute_values(cursor, upsert_sql, validated_rows)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise AirflowFailException(f"Erro no Bulk Upsert: {e}")
            finally:
                cursor.close()
                conn.close()

        return len(validated_rows)


    @task
    def final_quality_check(initial_timestamp):
        """
        Delta-Based Validation:
        Only updates the Dataset (outlets) if real changes are detected.
        """
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        
        if initial_timestamp:
            check_query = "SELECT COUNT(*) FROM bronze.breweries WHERE updated_at > %s"
            params = (initial_timestamp,)
        else:
            check_query = "SELECT COUNT(*) FROM bronze.breweries"
            params = None
            
        actual_updated = pg_hook.get_first(check_query, parameters=params)[0]
        total_count = pg_hook.get_first("SELECT COUNT(*) FROM bronze.breweries")[0]
        
        logging.info(f"DQ Bronze -> Total: {total_count} | New/Changed in this batch: {actual_updated}")
        
        if total_count == 0:
            raise AirflowFailException("DQ Fail: The Bronze table is empty.")
        
        if actual_updated == 0:
            # If no changes are detected, skip execution to avoid unnecessary downstream triggers
            raise AirflowSkipException("No changes detected in the API. Silver will not be triggered.")
            
        return actual_updated

    
    @task(outlets=[BRONZE_BREWERIES_DATASET])
    def trigger_dataset_update(count):
        logging.info(f"Signaling update of {count} records to the Silver Layer.")

    # Orchestration Workflow
    health = health_check_supabase()
    setup = setup_database()
    initial_ts = get_initial_state()
    pages = get_pages_to_process()
    
    health >> setup >> initial_ts
    
    ingestion = fetch_and_validate_to_bronze.expand(page=pages)
    
    
    quality_result = final_quality_check(initial_ts)
    
    initial_ts >> ingestion >> quality_result >> trigger_dataset_update(quality_result)

brewery_dag = brewery_bronze_pipeline()