from decimal import Decimal

from automotive_data_project.transformation.cleaning import (
    clean_engine_capacity,
    clean_mileage,
    clean_power_hp,
    clean_price,
)


def test_clean_price() -> None:
    assert clean_price("89 900 PLN") == Decimal("89900")


def test_clean_mileage() -> None:
    assert clean_mileage("45 000 km") == 45000


def test_clean_power_hp_from_hp_and_kw() -> None:
    assert clean_power_hp("122 KM") == 122
    assert clean_power_hp("90 kW") == 122


def test_clean_engine_capacity() -> None:
    assert clean_engine_capacity("1 798 cm3") == 1798
