from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import os
import logging

# Importações do PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Dataset da Silver (O gatilho que acorda a Gold automaticamente)
SILVER_BREWERIES_DATASET = Dataset("postgres://supabase/silver/breweries")

def notify_gold_failure(context):
    """Notificação resiliente: não quebra a DAG se o SMTP não estiver configurado."""
    try:
        ti = context['task_instance']
        subject = f"🚨 Falha na Camada Gold: {ti.dag_id}"
        body = f"Erro na task {ti.task_id}. Verifique os logs: {ti.log_url}"
        send_email(to='rfucuhara1401@gmail.com', subject=subject, html_content=body)
        logging.info("Notificação de erro enviada.")
    except Exception as e:
        logging.error(f"Falha ao enviar e-mail de alerta: {e}")

default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'on_failure_callback': notify_gold_failure,
    'execution_timeout': timedelta(minutes=30)
}

@dag(
    dag_id='brewery_aggregation_gold_pyspark',
    default_args=default_args,
    schedule=[SILVER_BREWERIES_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bees', 'gold', 'pyspark', 'analytics', 'data_lake', 'snapshot'],
)
def brewery_gold_pipeline():

    @task
    def check_silver_delta():
        """
        Gatekeeper: Compara Silver (ingested_at) com Gold (processed_at).
        """
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        
        latest_silver = pg_hook.get_first("SELECT MAX(ingested_at) FROM silver.breweries")[0]
        
        if not latest_silver:
            raise AirflowFailException("DQ Fail: Camada Silver está vazia.")

        try:
            latest_gold = pg_hook.get_first("SELECT MAX(processed_at) FROM gold.brewery_counts")[0]
        except Exception:
            latest_gold = None

        logging.info(f"Gold Delta Check -> Silver: {latest_silver} | Gold: {latest_gold}")

        if latest_gold and latest_silver <= latest_gold:
            raise AirflowSkipException("Skip: A Camada Gold já está atualizada.")
        
        return True

    @task
    def aggregate_and_load_gold(**context):
        """
        Agregação Gold: Identifica a última pasta da Silver e gera a visão analítica.
        """
        run_id = context['logical_date'].strftime("%Y%m%d_%H%M")
        base_path_silver = '/opt/airflow/data/silver_breweries_pyspark'
        
        # --- Lógica de identificação da última execução bem-sucedida ---
        all_runs = [d for d in os.listdir(base_path_silver) if d.startswith('run_')]
        if not all_runs:
            raise AirflowFailException("Nenhuma pasta de execução 'run_' encontrada na Silver.")
        
        # Ordenamos para pegar o timestamp mais recente
        last_run_folder = sorted(all_runs)[-1]
        full_last_run_path = os.path.join(base_path_silver, last_run_folder)
        
        logging.info(f"Processando dados da última execução Silver: {last_run_folder}")

        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        pg_hook.run("CREATE SCHEMA IF NOT EXISTS gold;")
        
        spark = SparkSession.builder \
            .appName(f"BreweryGoldAggregation_{run_id}") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
            .getOrCreate()

        try:
            # --- AJUSTE NA LEITURA ---
            # Ao ler o diretório raiz da run sem o wildcard '/*', o Spark
            # identifica automaticamente as partições de diretório (country/state_province)
            df_silver = spark.read.parquet(full_last_run_path)
            
            silver_count = df_silver.count()
            logging.info(f"Registros lidos da Silver na pasta {last_run_folder}: {silver_count}")

            if silver_count == 0:
                raise AirflowFailException("A pasta Silver da rodada está vazia.")

            # 2. Agregação Analítica: Quantidade por tipo e localização 
            df_gold = df_silver.groupBy("country", "state_province", "brewery_type") \
                .agg(F.count("*").alias("total_breweries")) \
                .withColumn("processed_at", F.current_timestamp())

            gold_row_count = df_gold.count()

            # 3. Escrita no Banco (JDBC)
            conn = pg_hook.get_connection('supabase_conn')
            jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}?prepareThreshold=0"

            df_gold.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "gold.brewery_counts") \
                .option("user", conn.login) \
                .option("password", conn.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

            # 4. Escrita no Data Lake (Gold Parquet) 
            gold_output_path = f'/opt/airflow/data/gold_brewery_aggregation/run_{run_id}'
            
            # Coalesce(1) para gerar um arquivo consolidado na Gold (análise fim de linha)
            df_gold.coalesce(1).write \
                .mode("overwrite") \
                .parquet(gold_output_path)
            
            logging.info(f"Agregação Gold finalizada com {gold_row_count} grupos de registros.")
            return gold_row_count

        finally:
            spark.stop()

    @task
    def validate_gold_quality(expected_groups: int):
        if not expected_groups: return
        pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
        latest_ts = pg_hook.get_first("SELECT MAX(processed_at) FROM gold.brewery_counts")[0]
        actual_groups = pg_hook.get_first(
            "SELECT COUNT(*) FROM gold.brewery_counts WHERE processed_at = %s", 
            parameters=(latest_ts,)
        )[0]
        
        if actual_groups != expected_groups:
            raise AirflowFailException(f"Erro de integridade Gold: Esperado {expected_groups}, Banco {actual_groups}")

    # Fluxo de Orquestração
    check_delta = check_silver_delta()
    aggregation = aggregate_and_load_gold()
    
    check_delta >> aggregation >> validate_gold_quality(aggregation)

brewery_gold_dag = brewery_gold_pipeline()