from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from automotive_data_project.storage.database import init_schema, make_engine, make_session_factory
from automotive_data_project.storage.models import Listing
from automotive_data_project.storage.repositories import ListingRepository


def _record(price: Decimal = Decimal("89900")) -> dict[str, object]:
    return {
        "advert_id": "1001",
        "source": "otomoto",
        "source_url": "https://example.test/1001",
        "make": "Toyota",
        "model": "Corolla",
        "version": "1.8 Hybrid",
        "production_year": 2020,
        "price": price,
        "currency": "PLN",
        "mileage_km": 45000,
        "fuel_type": "Hybryda",
        "transmission": "Automatyczna",
        "body_type": "Sedan",
        "power_hp": 122,
        "engine_capacity_cm3": 1798,
        "advert_date": datetime(2026, 5, 12, 14, 20),
        "scraped_at": datetime.now(timezone.utc),
        "equipment": ["ABS"],
        "raw_parameters": {"Marka pojazdu": "Toyota"},
    }


def test_upsert_existing_offer_updates_without_duplicate() -> None:
    engine = make_engine("sqlite+pysqlite:///:memory:")
    init_schema(engine)
    session_factory = make_session_factory(engine)

    with session_factory.begin() as session:
        repo = ListingRepository(session)
        repo.upsert_many([_record()])
        first_seen = session.execute(select(Listing.first_seen_at)).scalar_one()
        repo.upsert_many([_record(price=Decimal("91000"))])
        listings = session.execute(select(Listing)).scalars().all()

    assert len(listings) == 1
    assert listings[0].price == Decimal("91000.00")
    assert listings[0].first_seen_at == first_seen
    assert listings[0].last_seen_at is not None
