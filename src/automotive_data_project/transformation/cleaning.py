from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

POLISH_MONTHS = {
    "stycznia": 1,
    "lutego": 2,
    "marca": 3,
    "kwietnia": 4,
    "maja": 5,
    "czerwca": 6,
    "lipca": 7,
    "sierpnia": 8,
    "września": 9,
    "października": 10,
    "listopada": 11,
    "grudnia": 12,
}


def digits_only(value: str | None) -> str:
    return re.sub(r"\D+", "", value or "")


def clean_price(value: str | None) -> Decimal | None:
    raw = digits_only(value)
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def clean_mileage(value: str | None) -> int | None:
    raw = digits_only(value)
    return int(raw) if raw else None


def clean_engine_capacity(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"([\d\s]+)(?:cm3|cm³|ccm)?", value.lower())
    if not match:
        return None
    raw = digits_only(match.group(1))
    return int(raw) if raw else None


def clean_power_hp(value: str | None) -> int | None:
    if not value:
        return None
    compact = value.replace(" ", "").replace(",", ".").lower()
    match = re.search(r"(\d+(?:\.\d+)?)(kw|km|hp)?", compact)
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2)
    if unit == "kw":
        number *= 1.35962
    return round(number)


def clean_int(value: str | None) -> int | None:
    raw = digits_only(value)
    return int(raw) if raw else None


def parse_polish_advert_date(value: str | None) -> datetime | None:
    if not value:
        return None
    match = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})(?:\s+(\d{1,2}):(\d{2}))?", value.lower())
    if not match:
        return None
    day, month_name, year, hour, minute = match.groups()
    month = POLISH_MONTHS.get(month_name)
    if not month:
        return None
    return datetime(int(year), month, int(day), int(hour or 0), int(minute or 0))
