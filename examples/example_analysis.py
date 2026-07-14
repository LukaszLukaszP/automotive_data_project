from __future__ import annotations

import os
from collections import defaultdict
from decimal import Decimal
from statistics import mean, median

from sqlalchemy import create_engine, text


def money(value: Decimal | float | int | None) -> float | None:
    return float(value) if value is not None else None


def main() -> None:
    database_url = os.environ.get("DATABASE_URL", "sqlite:///data/automotive_data.sqlite3")
    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT make, model, version, production_year, price, mileage_km, fuel_type
                FROM listings
                WHERE price IS NOT NULL
                """
            )
        ).mappings().all()

    if not rows:
        print("No listings with price found. Run the pipeline first.")
        return

    by_year: dict[int, list[float]] = defaultdict(list)
    by_fuel: dict[str, list[float]] = defaultdict(list)
    by_make_model: dict[tuple[str, str], int] = defaultdict(int)
    mileage_pairs: list[tuple[int, float]] = []

    for row in rows:
        price = money(row["price"])
        if price is None:
            continue
        if row["production_year"]:
            by_year[int(row["production_year"])].append(price)
        by_fuel[row["fuel_type"] or "unknown"].append(price)
        by_make_model[(row["make"] or "unknown", row["model"] or "unknown")] += 1
        if row["mileage_km"] is not None:
            mileage_pairs.append((int(row["mileage_km"]), price))

    print("\nMedian price by production year")
    for year, prices in sorted(by_year.items()):
        print(f"{year}: {median(prices):,.0f}")

    print("\nAverage price by fuel type")
    for fuel, prices in sorted(by_fuel.items(), key=lambda item: item[0]):
        print(f"{fuel}: {mean(prices):,.0f}")

    print("\nOffer count by make/model")
    for (make, model), count in sorted(by_make_model.items(), key=lambda item: item[1], reverse=True):
        print(f"{make} {model}: {count}")

    print("\nAverage price by mileage bucket")
    buckets: dict[str, list[float]] = defaultdict(list)
    for mileage, price in mileage_pairs:
        bucket_start = mileage // 50000 * 50000
        bucket = f"{bucket_start:06d}-{bucket_start + 49999:06d} km"
        buckets[bucket].append(price)
    for bucket, prices in sorted(buckets.items()):
        print(f"{bucket}: {mean(prices):,.0f}")


if __name__ == "__main__":
    main()
