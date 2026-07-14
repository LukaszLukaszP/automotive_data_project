from pathlib import Path

from automotive_data_project.scraping.parser import (
    is_captcha_html,
    parse_listing_page,
    parse_offer_page,
    parse_total_pages,
)
from automotive_data_project.transformation.normalization import normalize_listing

FIXTURES = Path(__file__).parent / "fixtures"


def read_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_extract_listing_ids_and_links() -> None:
    refs = parse_listing_page(read_fixture("listing_page.html"))

    assert [ref.advert_id for ref in refs] == ["1001", "1002"]
    assert refs[0].url == "https://www.otomoto.pl/osobowe/oferta/toyota-corolla-ID1001.html"


def test_parse_total_pages() -> None:
    assert parse_total_pages(read_fixture("listing_page.html")) == 2


def test_parse_basic_parameters_with_changed_order() -> None:
    raw = parse_offer_page(read_fixture("offer_complete.html"), "https://example.test/offer", "1001")

    assert raw.raw_fields["Marka pojazdu"] == "Toyota"
    assert raw.raw_fields["Model pojazdu"] == "Corolla"
    assert raw.raw_fields["Przebieg"] == "45 000 km"
    assert raw.equipment == ["ABS", "Apple CarPlay"]


def test_missing_field_does_not_fail() -> None:
    raw = parse_offer_page(read_fixture("offer_missing_field.html"), "https://example.test/offer", "1003")
    normalized = normalize_listing(raw)

    assert normalized["make"] == "Toyota"
    assert normalized["mileage_km"] is None


def test_captcha_detection() -> None:
    assert is_captcha_html(read_fixture("captcha.html"))
