import unittest
from airflow.models import DagBag

class TestDagIntegrity(unittest.TestCase):
    def test_dagbag_no_errors(self):
    
        dag_bag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False)
        
        errors = dag_bag.import_errors
        msg = f"DAG import errors: {errors}"
        
        self.assertEqual(len(errors), 0, msg)
        print("\n✅ Sucesso: Nenhuma falha de integridade nas DAGs!")

if __name__ == "__main__":
    unittest.main()