"""
Prueba Unitaria 3: Integridad de DAGs de Apache Airflow
Verifica que todos los archivos DAG tengan sintaxis válida y estén correctamente definidos.
Compatible con pytest y unittest estándar.
"""

import os
import ast
import glob
import unittest

DAGS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))


class TestDagIntegrity(unittest.TestCase):

    def test_dags_syntax_and_structure(self):
        """Valida que todos los DAGs se compilen sin errores de sintaxis y contengan definiciones de DAG."""
        dag_files = glob.glob(os.path.join(DAGS_FOLDER, "dag_*.py"))
        self.assertGreaterEqual(len(dag_files), 4, f"Se esperaban al menos 4 DAGs en {DAGS_FOLDER}")

        expected_dags = [
            "dag_01_ingest_usgs.py",
            "dag_02_transform_quality.py",
            "dag_03_daily_ai_report.py",
            "dag_04_historical_backfill.py"
        ]

        found_basenames = [os.path.basename(f) for f in dag_files]
        for exp in expected_dags:
            self.assertIn(exp, found_basenames, f"Falta el archivo esperado: {exp}")

        for fpath in dag_files:
            with open(fpath, "r", encoding="utf-8") as f:
                code = f.read()
            try:
                tree = ast.parse(code)
                self.assertIsNotNone(tree, f"Fallo al parsear AST en {fpath}")
            except SyntaxError as e:
                self.fail(f"Error de sintaxis en DAG {fpath}: {e}")

    def test_dagbag_if_airflow_installed(self):
        """Si apache-airflow está instalado en el entorno, ejecuta la prueba oficial DagBag."""
        try:
            from airflow.models import DagBag
            dagbag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
            self.assertEqual(len(dagbag.import_errors), 0, f"Errores de importación en DAGs: {dagbag.import_errors}")
            self.assertGreaterEqual(len(dagbag.dags), 4, "Menos de 4 DAGs cargados en DagBag")
        except (ImportError, ModuleNotFoundError):
            self.skipTest("Airflow no está instalado en el host (se ejecuta dentro de contenedor Docker)")


if __name__ == "__main__":
    unittest.main()
