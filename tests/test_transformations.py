"""
Prueba Unitaria 1: Funciones de Transformación y Enriquecimiento
Verifica la conversión de un payload GeoJSON de USGS a un registro tipado Medallón.
Compatible con pytest y unittest estándar.
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "airflow", "include")))

from transformations import (
    transformar_evento_geojson,
    clasificar_alerta,
    clasificar_profundidad,
    extraer_pais_region
)


class TestTransformations(unittest.TestCase):

    def test_transformar_evento_geojson_valido(self):
        mock_feature = {
            "type": "Feature",
            "id": "nc75001234",
            "properties": {
                "mag": 6.2,
                "place": "12 km SSW of Eureka, California",
                "time": 1700000000000,
                "updated": 1700001000000,
                "tsunami": 1,
                "sig": 620,
                "magType": "mww",
                "status": "reviewed",
                "type": "earthquake",
                "url": "https://earthquake.usgs.gov/earthquakes/eventpage/nc75001234"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-124.2, 40.8, 15.4]  # [lon, lat, depth]
            }
        }

        result = transformar_evento_geojson(mock_feature)

        self.assertEqual(result["id"], "nc75001234")
        self.assertEqual(result["mag"], 6.2)
        self.assertEqual(result["country_region"], "California")
        self.assertEqual(result["longitude"], -124.2)
        self.assertEqual(result["latitude"], 40.8)
        self.assertEqual(result["depth_km"], 15.4)
        self.assertEqual(result["alert_level"], "CRITICO")
        self.assertEqual(result["depth_type"], "SUPERFICIAL")
        self.assertEqual(result["tsunami"], 1)

    def test_clasificaciones_auxiliares(self):
        self.assertEqual(clasificar_alerta(6.5), "CRITICO")
        self.assertEqual(clasificar_alerta(5.2), "MODERADO")
        self.assertEqual(clasificar_alerta(3.8), "LEVE")

        self.assertEqual(clasificar_profundidad(20.0), "SUPERFICIAL")
        self.assertEqual(clasificar_profundidad(150.0), "INTERMEDIA")
        self.assertEqual(clasificar_profundidad(450.0), "PROFUNDA")

        self.assertEqual(extraer_pais_region("Off the coast of Bio-Bio, Chile"), "Chile")
        self.assertEqual(extraer_pais_region(None), "Global")


if __name__ == "__main__":
    unittest.main()
