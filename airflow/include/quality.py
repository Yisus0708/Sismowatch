"""
Motor de Reglas de Calidad de Datos Sísmicos
Proyecto: SismoWatch
"""

from typing import Dict, Any, Tuple


def validar_registro_sismico(record: Dict[str, Any]) -> Tuple[bool, str, str]:
    """
    Evalúa las reglas de calidad sobre un registro sísmico transformado.
    Retorna: (es_valido, motivo_rechazo, regla_fallida)
    """
    # 1. Campos obligatorios no nulos
    if not record.get("id") or record.get("id") == "UNKNOWN_ID":
        return False, "Campo 'id' nulo o inválido", "mandatory_fields_present"

    if record.get("time_utc") is None and record.get("time_epoch") is None:
        return False, "Marcador de tiempo ausente", "mandatory_fields_present"

    lat = record.get("latitude")
    lon = record.get("longitude")
    if lat is None or lon is None:
        return False, "Coordenadas geográficas nulas", "mandatory_fields_present"

    # 2. Rango de Coordenadas WGS84
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (ValueError, TypeError):
        return False, f"Coordenadas no numéricas: lat={lat}, lon={lon}", "valid_coordinate_types"

    if lat_f < -90.0 or lat_f > 90.0:
        return False, f"Latitud fuera de rango [-90, 90]: {lat_f}", "valid_latitude_range"

    if lon_f < -180.0 or lon_f > 180.0:
        return False, f"Longitud fuera de rango [-180, 180]: {lon_f}", "valid_longitude_range"

    # 3. Verosimilitud de Magnitud y Profundidad
    mag = record.get("mag")
    if mag is not None:
        try:
            mag_f = float(mag)
            if mag_f < -2.0 or mag_f > 10.0:
                return False, f"Magnitud fuera del rango físico plausible [-2, 10]: {mag_f}", "plausible_magnitude"
        except (ValueError, TypeError):
            return False, f"Magnitud no numérica: {mag}", "valid_magnitude_type"

    depth = record.get("depth_km")
    if depth is not None:
        try:
            depth_f = float(depth)
            if depth_f < -5.0 or depth_f > 800.0:
                return False, f"Profundidad hipocentral fuera de rango [-5, 800] km: {depth_f}", "plausible_depth"
        except (ValueError, TypeError):
            return False, f"Profundidad no numérica: {depth}", "valid_depth_type"

    return True, "", ""
