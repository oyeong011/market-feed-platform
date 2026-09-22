from __future__ import annotations

import datetime as dt

from .types import MigrationPreflightError

UTC = dt.timezone.utc


def epoch_us_to_datetime(value: int) -> dt.datetime:
    seconds, micros = divmod(value, 1_000_000)
    return dt.datetime.fromtimestamp(seconds, UTC).replace(microsecond=micros)


def datetime_to_epoch_us(value: dt.datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise MigrationPreflightError("datetime value is missing timezone")
    utc_value = value.astimezone(UTC)
    return (
        int(utc_value.timestamp()) * 1_000_000
        + utc_value.microsecond
    )


def parse_aware_text(value: str) -> int:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise MigrationPreflightError(
            "instrument timestamp text must be ISO-8601 with timezone"
        ) from exc
    return datetime_to_epoch_us(parsed)
