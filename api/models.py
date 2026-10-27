"""
Modelos de Datos y Esquemas Pydantic
Proyecto: SismoWatch API
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str


class KPIResponse(BaseModel):
    total_sismos_24h: int
    magnitud_maxima: float
    magnitud_promedio: float
    total_tsunamis: int
    sismos_criticos_m6: int
    sismos_peligrosos_m5: int
    puntaje_calidad: float
    ultima_actualizacion_utc: str


class BackfillTriggerRequest(BaseModel):
    days_back: int = Field(30, ge=1, le=90)
    min_magnitude: float = Field(2.5, ge=1.0, le=8.0)
