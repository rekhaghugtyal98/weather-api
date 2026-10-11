"""Shared helpers."""
from datetime import datetime, timezone
import re

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_identifier(value: str) -> str:
    """Reject SQL identifier injection in configured catalog/schema names."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return value
