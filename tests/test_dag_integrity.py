import pytest
from airflow.models import DagBag

def test_dag_loaded_self_check():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    # Verifica se há erros de importação nas DAGs
    assert len(dagbag.import_errors) == 0, f"Erros encontrados: {dagbag.import_errors}"

def test_dag_ids():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    # Garante que suas DAGs principais estão presentes
    expected_dags = ['brewery_ingestion_bronze', 'brewery_transformation_silver_pyspark', 'brewery_aggregation_gold_pyspark']
    for dag_id in expected_dags:
        assert dag_id in dagbag.dags