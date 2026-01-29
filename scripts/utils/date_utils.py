from datetime import datetime
from typing import Optional


def get_canonical_data_date(data_date: Optional[str] = None) -> str:
    """
    Returns a canonical, filename-safe data_date string.
    If a value is provided, it is returned unchanged (assumed already canonical).
    Otherwise, generate one using UTC now in format YYYYMMDD_HHMMSS.
    """
    if data_date and isinstance(data_date, str) and data_date.strip():
        return data_date
    # Use local time per existing flows; switch to utcnow() if needed
    return datetime.now().strftime("%Y%m%d_%H%M%S")
