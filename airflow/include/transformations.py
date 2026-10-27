"""
Módulo de Transformaciones Puras de Datos Sísmicos
Proyecto: SismoWatch
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional


def clasificar_alerta(magnitud: float) -> str:
    """Clasifica el nivel de alerta según la magnitud del sismo."""
    if magnitud >= 6.0:
        return "CRITICO"
    elif magnitud >= 5.0:
        return "MODERADO"
    return "LEVE"


def clasificar_profundidad(profundidad_km: float) -> str:
    """Clasifica el hipocentro según su profundidad geofísica."""
    if profundidad_km <= 70.0:
        return "SUPERFICIAL"
    elif profundidad_km <= 300.0:
        return "INTERMEDIA"
    return "PROFUNDA"


def extraer_pais_region(place: Optional[str]) -> str:
    """Extrae y normaliza el país o región tras la última coma de 'place'."""
    if not place:
        return "Global"
    partes = place.split(",")
    region = partes[-1].strip()
    return region if region else "Global"


def transformar_evento_geojson(feature: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convierte una feature GeoJSON de la API de USGS a un registro estructurado y tipado.
    """
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry", {}) or {}
    coords = geom.get("coordinates", [0.0, 0.0, 0.0]) or [0.0, 0.0, 0.0]

    event_id = feature.get("id") or props.get("code") or "UNKNOWN_ID"
    mag = float(props.get("mag") or 0.0)
    time_epoch = props.get("time") or int(datetime.now(timezone.utc).timestamp() * 1000)
    time_utc = datetime.fromtimestamp(time_epoch / 1000.0, tz=timezone.utc).replace(tzinfo=None)

    lon = float(coords[0]) if len(coords) > 0 else 0.0
    lat = float(coords[1]) if len(coords) > 1 else 0.0
    depth = float(coords[2]) if len(coords) > 2 else 0.0

    place = props.get("place") or "Ubicación no especificada"
    country_region = extraer_pais_region(place)

    return {
        "id": event_id,
        "mag": round(mag, 2),
        "mag_type": props.get("magType", "unknown"),
        "place": place,
        "country_region": country_region,
        "time_epoch": time_epoch,
        "time_utc": time_utc,
        "updated_epoch": props.get("updated"),
        "longitude": round(lon, 4),
        "latitude": round(lat, 4),
        "depth_km": round(depth, 2),
        "tsunami": int(props.get("tsunami") or 0),
        "sig": int(props.get("sig") or 0),
        "status": props.get("status", "automatic"),
        "type": props.get("type", "earthquake"),
        "url": props.get("url", ""),
        "alert_level": clasificar_alerta(mag),
        "depth_type": clasificar_profundidad(depth)
    }
