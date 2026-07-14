from __future__ import annotations

from sqlalchemy import JSON, DateTime, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Listing(Base):
    __tablename__ = "listings"
    __table_args__ = (Index("uq_listings_source_advert_id", "source", "advert_id", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    advert_id: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="otomoto")
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    make: Mapped[str | None] = mapped_column(String(100))
    model: Mapped[str | None] = mapped_column(String(100))
    version: Mapped[str | None] = mapped_column(String(255))
    production_year: Mapped[int | None] = mapped_column(Integer)
    price: Mapped[object | None] = mapped_column(Numeric(12, 2))
    currency: Mapped[str | None] = mapped_column(String(10))
    mileage_km: Mapped[int | None] = mapped_column(Integer)
    fuel_type: Mapped[str | None] = mapped_column(String(100))
    transmission: Mapped[str | None] = mapped_column(String(100))
    body_type: Mapped[str | None] = mapped_column(String(100))
    power_hp: Mapped[int | None] = mapped_column(Integer)
    engine_capacity_cm3: Mapped[int | None] = mapped_column(Integer)
    advert_date: Mapped[object | None] = mapped_column(DateTime)
    first_seen_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_seen_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    scraped_at: Mapped[object | None] = mapped_column(DateTime(timezone=True))
    equipment: Mapped[list[str] | None] = mapped_column(JSON)
    raw_parameters: Mapped[dict[str, str] | None] = mapped_column(JSON)
