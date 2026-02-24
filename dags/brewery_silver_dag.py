from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.email import send_email
from airflow.utils import timezone
from airflow.configuration import conf
from datetime import datetime, timedelta
import os
import logging

# PySpark Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Datasets for event-driven orchestration
BRONZE_BREWERIES_DATASET = Dataset("postgres://supabase/bronze/breweries")
SILVER_BREWERIES_DATASET = Dataset("postgres://supabase/silver/breweries")

def notify_silver_failure(context):
    ti = context['task_instance']
    receiver_email = conf.get('smtp', 'smtp_user')
    subject = f"⚠️ Silver Transformation Failure: {ti.dag_id}"
    body = f"Task failure {ti.task_id}. View logs at: {ti.log_url}"
    send_email(to=receiver_email, subject=subject, html_content=body)

default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'on_failure_callback': notify_silver_failure,
    'execution_timeout': timedelta(minutes=30),
}

@dag(
    dag_id='brewery_transformation_silver_pyspark',
    default_args=default_args,
    schedule=[BRONZE_BREWERIES_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bees', 'silver', 'pyspark', 'partitioned', 'snapshot'],
)
def brewery_silver_pipeline():

    @task
    def check_bronze_delta():
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        latest_bronze = pg_hook.get_first("SELECT MAX(updated_at) FROM bronze.breweries")[0]
        
        if not latest_bronze:
            raise AirflowFailException("DQ Fail: Bronze layer is empty.")

        try:
            latest_silver = pg_hook.get_first("SELECT MAX(ingested_at) FROM silver.breweries")[0]
        except Exception:
            latest_silver = None

        if latest_bronze and latest_bronze.tzinfo is None:
            latest_bronze = latest_bronze.replace(tzinfo=timezone.utc)
        
        if latest_silver and latest_silver.tzinfo is None:
            latest_silver = latest_silver.replace(tzinfo=timezone.utc)

        if latest_silver and latest_bronze <= latest_silver:
            raise AirflowSkipException("Skip: No new data detected in the Bronze layer.")
        
        return latest_silver.isoformat() if latest_silver else '1900-01-01T00:00:00'

    @task(outlets=[SILVER_BREWERIES_DATASET])
    def process_and_load_with_spark(last_processed_ts: str, **context):
        """
        Silver Processing: Partitioning by location as per case requirements
        """
        run_id = context['logical_date'].strftime("%Y%m%d_%H%M")
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        pg_hook.run("CREATE SCHEMA IF NOT EXISTS silver;")
        
        spark = SparkSession.builder \
            .appName(f"BrewerySilver_Incremental_{run_id}") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
            .getOrCreate()

        try:
            conn = pg_hook.get_connection('supabase_conn')
            jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}?prepareThreshold=0"
            sql_delta = f"(SELECT * FROM bronze.breweries WHERE updated_at > '{last_processed_ts}') AS delta_data"
            
            df_raw = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", sql_delta) \
                .option("user", conn.login) \
                .option("password", conn.password) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            row_count = df_raw.count()
            if row_count == 0:
                return 0

            # Transformation and Data Cleansing
            df_transformed = df_raw.withColumn("data", F.from_json(F.col("payload"), 
                "id STRING, name STRING, brewery_type STRING, city STRING, state_province STRING, country STRING, phone STRING, website_url STRING, latitude STRING, longitude STRING, address_1 STRING, address_2 STRING, address_3 STRING")) \
                .select("data.*") \
                .withColumn("latitude", F.col("latitude").cast("double")) \
                .withColumn("longitude", F.col("longitude").cast("double")) \
                .withColumn("country", F.initcap(F.trim(F.coalesce(F.col("country"), F.lit("Not Informed"))))) \
                .withColumn("state_province", F.initcap(F.trim(F.coalesce(F.col("state_province"), F.lit("Not Informed"))))) \
                .withColumn("city", F.initcap(F.trim(F.coalesce(F.col("city"), F.lit("Not Informed"))))) \
                .withColumn("brewery_type", F.initcap(F.trim(F.col("brewery_type")))) \
                .withColumn("phone", F.regexp_replace(F.col("phone"), r"\D", "")) \
                .withColumn("website_url", F.lower(F.trim(F.col("website_url")))) \
                .withColumn("full_address", F.concat_ws(" ", F.col("address_1"), F.col("address_2"), F.col("address_3"))) \
                .withColumn("ingested_at", F.current_timestamp())

            # 1. Writing to Relational Database
            df_transformed.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "silver.breweries") \
                .option("user", conn.login) \
                .option("password", conn.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            # 2. Partitioned Parquet Write by Location (PDF Requirement)
            output_path = f'/opt/airflow/data/silver_breweries_pyspark/run_{run_id}'
            

            df_transformed.write \
                .mode("overwrite") \
                .partitionBy("country", "state_province") \
                .parquet(output_path)
            
            logging.info(f"Lote {run_id} salvo e particionado em: {output_path}")
            return row_count

        finally:
            spark.stop()

    @task
    def validate_silver_quality(expected_count: int):
        if expected_count == 0:
            return
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        latest_ts = pg_hook.get_first("SELECT MAX(ingested_at) FROM silver.breweries")[0]
        actual_count = pg_hook.get_first("SELECT COUNT(*) FROM silver.breweries WHERE ingested_at = %s", parameters=(latest_ts,))[0]
        if actual_count != expected_count:
            raise AirflowFailException(f"Data Divergence: {expected_count} vs {actual_count}")

    last_ts = check_bronze_delta()
    spark_job = process_and_load_with_spark(last_ts)
    validate_silver_quality(spark_job)

brewery_silver_dag = brewery_silver_pipeline()