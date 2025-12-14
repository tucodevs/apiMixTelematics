from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

try:
    from zoneinfo import ZoneInfo  # type: ignore
except ImportError:  # pragma: no cover - fallback only for very old Python
    ZoneInfo = None  # type: ignore

FUSO_MANAUS = timezone(timedelta(hours=-4))
MAX_DIAS_PADRAO = 7


def token_para_datetime(token: Optional[str]) -> Optional[datetime]:
    """Converte o since_token (string) para datetime em UTC."""
    if not token:
        return None
    try:
        dt = datetime.strptime(token[:14], "%Y%m%d%H%M%S")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def datetime_para_token(dt: datetime) -> str:
    """Recebe um datetime timezone-aware (qualquer fuso) e devolve o since_token."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y%m%d%H%M%S") + "000"


def gerar_token_relativo_info(horas_atras: int = 24):
    """Retorna token + horários referência (Manaus e UTC)."""
    dt_manaus = datetime.now(FUSO_MANAUS) - timedelta(hours=horas_atras)
    dt_utc = dt_manaus.astimezone(timezone.utc)
    token = datetime_para_token(dt_manaus)
    return token, dt_manaus, dt_utc


def gerar_token_relativo(horas_atras: int = 24) -> str:
    """Gera um token relativo à data/hora atual de Manaus."""
    token, _, _ = gerar_token_relativo_info(horas_atras)
    return token


def traduzir_token(token: Optional[str], tz=FUSO_MANAUS, formato: str = "%d/%m/%Y %H:%M:%S") -> str:
    dt = token_para_datetime(token)
    if not dt:
        return "inválido"
    return dt.astimezone(tz).strftime(formato)


def formatar_timedelta(delta: timedelta) -> str:
    dias = delta.days
    horas = delta.seconds // 3600
    minutos = (delta.seconds % 3600) // 60
    partes = []
    if dias:
        partes.append(f"{dias}d")
    if horas or dias:
        partes.append(f"{horas}h")
    partes.append(f"{minutos}m")
    return " ".join(partes)


def validar_idade_token(
    token: Optional[str], max_dias: int = MAX_DIAS_PADRAO
) -> Tuple[bool, Optional[datetime], Optional[timedelta], timedelta]:
    """Retorna se o token está dentro do limite de dias aceito na API."""
    limite = timedelta(days=max_dias)
    dt = token_para_datetime(token)
    if not dt:
        return False, None, None, limite
    idade = datetime.now(timezone.utc) - dt
    return idade <= limite, dt, idade, limite


def formatar_token_curto(token: Optional[str]) -> str:
    if not token:
        return "vazio"
    if len(token) <= 10:
        return token
    return f"{token[:6]}...{token[-4:]} (len={len(token)})"


def timezone_from_name(nome: Optional[str]):
    if not nome or ZoneInfo is None:
        return None
    try:
        return ZoneInfo(nome)
    except Exception:
        return None
