"""
Módulo de Seguridad, Autenticación JWT y Control de Acceso por Roles (RBAC)
Proyecto: SismoWatch API
"""

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "super_secret_jwt_key_sismowatch_2026_x99")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
VIEWER_USER = os.getenv("VIEWER_USER", "viewer")
VIEWER_PASSWORD = os.getenv("VIEWER_PASSWORD", "viewer123")

security_scheme = HTTPBearer(auto_error=False)


def authenticate_user(username: str, password: str) -> Optional[Dict[str, str]]:
    if username == ADMIN_USER and password == ADMIN_PASSWORD:
        return {"username": ADMIN_USER, "role": "admin"}
    if username == VIEWER_USER and password == VIEWER_PASSWORD:
        return {"username": VIEWER_USER, "role": "viewer"}
    return None


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_scheme)) -> Dict[str, Any]:
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No autenticado. Se requiere un token Bearer válido para acceder a este recurso.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if username is None or role is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token no contiene los reclamos de identidad o rol requeridos.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return {"username": username, "role": role}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autenticación expirado o inválido.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_admin(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    if user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acceso restringido: Se requieren privilegios de Administrador para ejecutar esta acción."
        )
    return user


def sanitize_prompt_text(text: str) -> str:
    """
    Mitigación de inyecciones de instrucciones (Prompt Injection):
    Elimina caracteres de control y secuencias comunes de jailbreak.
    """
    if not text:
        return ""
    # Remover etiquetas y patrones típicos de inyección
    cleaned = re.sub(r"(ignore\s+all\s+previous\s+instructions|system\s+prompt|dan\s+mode)", "[REDACTED]", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"[<>{}\[\]\\]", "", cleaned)
    return cleaned.strip()[:300]
