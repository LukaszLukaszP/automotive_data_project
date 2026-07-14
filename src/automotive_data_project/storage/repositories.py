from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from automotive_data_project.storage.models import Listing

UPSERT_COLUMNS = [
    "source_url",
    "make",
    "model",
    "version",
    "production_year",
    "price",
    "currency",
    "mileage_km",
    "fuel_type",
    "transmission",
    "body_type",
    "power_hp",
    "engine_capacity_cm3",
    "advert_date",
    "scraped_at",
    "equipment",
    "raw_parameters",
]


def _serialize(value: object) -> object:
    if isinstance(value, Decimal):
        return value
    return value


def _payload(record: dict[str, object]) -> dict[str, object]:
    payload = {key: _serialize(value) for key, value in record.items()}
    now = datetime.now(timezone.utc)
    payload.setdefault("first_seen_at", now)
    payload["last_seen_at"] = now
    return payload


class ListingRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def existing_advert_ids(self, source: str = "otomoto") -> set[str]:
        rows = self.session.execute(select(Listing.advert_id).where(Listing.source == source)).all()
        return {row[0] for row in rows}

    def touch_seen(self, source: str, advert_ids: set[str]) -> int:
        """Update last_seen_at for existing adverts encountered again."""
        if not advert_ids:
            return 0
        now = datetime.now(timezone.utc)
        result = self.session.execute(
            update(Listing)
            .where(Listing.source == source, Listing.advert_id.in_(advert_ids))
            .values(last_seen_at=now)
        )
        return int(result.rowcount or 0)

    def upsert_many(self, records: list[dict[str, object]]) -> int:
        if not records:
            return 0
        dialect = self.session.bind.dialect.name if self.session.bind is not None else ""
        statement_factory = pg_insert if dialect == "postgresql" else sqlite_insert
        count = 0
        for record in records:
            values = _payload(record)
            insert_stmt = statement_factory(Listing).values(**values)
            update_values = {column: getattr(insert_stmt.excluded, column) for column in UPSERT_COLUMNS}
            update_values["last_seen_at"] = values["last_seen_at"]
            statement = insert_stmt.on_conflict_do_update(
                index_elements=["source", "advert_id"],
                set_=update_values,
            )
            self.session.execute(statement)
            count += 1
        return count
