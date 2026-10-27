"""
Prueba Unitaria 2: Reglas de Calidad de Datos
Valida el motor de calidad frente a un registro válido y registros anómalos destinados a cuarentena.
Compatible con pytest y unittest estándar.
"""

import sys
import os
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "airflow", "include")))

from quality import validar_registro_sismico


class TestQualityRules(unittest.TestCase):

    def test_calidad_caso_valido(self):
        registro_valido = {
            "id": "us7000abcd",
            "time_utc": datetime.now(timezone.utc),
            "time_epoch": 1700000000000,
            "latitude": -33.4569,
            "longitude": -70.6483,
            "mag": 5.4,
            "depth_km": 45.0
        }

        es_valido, motivo, regla = validar_registro_sismico(registro_valido)
        self.assertTrue(es_valido)
        self.assertEqual(motivo, "")
        self.assertEqual(regla, "")

    def test_calidad_caso_invalido_latitud_fuera_de_rango(self):
        registro_lat_invalida = {
            "id": "us7000err1",
            "time_utc": datetime.now(timezone.utc),
            "time_epoch": 1700000000000,
            "latitude": 125.0,  # Inválido: latitud debe estar en [-90, 90]
            "longitude": -70.6483,
            "mag": 4.5,
            "depth_km": 20.0
        }

        es_valido, motivo, regla = validar_registro_sismico(registro_lat_invalida)
        self.assertFalse(es_valido)
        self.assertIn("Latitud fuera de rango", motivo)
        self.assertEqual(regla, "valid_latitude_range")

    def test_calidad_caso_invalido_campos_obligatorios_nulos(self):
        registro_sin_id = {
            "id": None,
            "time_utc": datetime.now(timezone.utc),
            "latitude": 10.0,
            "longitude": -10.0
        }

        es_valido, motivo, regla = validar_registro_sismico(registro_sin_id)
        self.assertFalse(es_valido)
        self.assertEqual(regla, "mandatory_fields_present")


if __name__ == "__main__":
    unittest.main()
