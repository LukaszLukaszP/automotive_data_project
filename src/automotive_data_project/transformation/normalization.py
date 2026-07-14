from __future__ import annotations

from automotive_data_project.scraping.models import RawListing
from automotive_data_project.transformation.cleaning import (
    clean_engine_capacity,
    clean_int,
    clean_mileage,
    clean_power_hp,
    clean_price,
    parse_polish_advert_date,
)

LABEL_MAP = {
    "Marka pojazdu": "make",
    "Model pojazdu": "model",
    "Wersja": "version",
    "Rok produkcji": "production_year",
    "Rodzaj paliwa": "fuel_type",
    "Skrzynia biegów": "transmission",
    "Typ nadwozia": "body_type",
}


def normalize_listing(raw: RawListing) -> dict[str, object]:
    """Convert one raw listing into database-ready values."""
    fields = raw.raw_fields
    record: dict[str, object] = {
        "advert_id": raw.advert_id,
        "source": raw.source,
        "source_url": raw.source_url,
        "currency": raw.currency,
        "price": clean_price(raw.price_raw),
        "mileage_km": clean_mileage(fields.get("Przebieg")),
        "power_hp": clean_power_hp(fields.get("Moc")),
        "engine_capacity_cm3": clean_engine_capacity(fields.get("Pojemność skokowa")),
        "production_year": clean_int(fields.get("Rok produkcji")),
        "advert_date": parse_polish_advert_date(raw.advert_date_raw),
        "scraped_at": raw.scraped_at,
        "equipment": raw.equipment,
        "raw_parameters": fields,
    }
    for source_label, target_name in LABEL_MAP.items():
        if target_name not in record:
            record[target_name] = fields.get(source_label)
    return record
