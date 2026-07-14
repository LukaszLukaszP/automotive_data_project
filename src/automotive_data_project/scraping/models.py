from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class ListingRef:
    advert_id: str
    url: str


@dataclass(frozen=True)
class RawListing:
    advert_id: str
    source: str
    source_url: str
    scraped_at: datetime
    raw_fields: dict[str, str] = field(default_factory=dict)
    equipment: list[str] = field(default_factory=list)
    price_raw: str | None = None
    currency: str | None = None
    advert_date_raw: str | None = None
