"""Shared helpers."""
from datetime import datetime, timezone
import re

def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO 8601 format.

    Returns:
        The current timezone-aware UTC timestamp as a string.
    """
    return datetime.now(timezone.utc).isoformat()

def safe_identifier(value: str) -> str:
    """Validate a catalog or schema name before interpolating it into SQL.

    Args:
        value: Identifier to validate.

    Returns:
        The unchanged identifier when it matches the allowed alphanumeric and
        underscore pattern and begins with a letter or underscore.

    Raises:
        ValueError: If ``value`` is not a valid simple SQL identifier.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return value
