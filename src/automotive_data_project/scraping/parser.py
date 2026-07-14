from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from automotive_data_project.scraping.models import ListingRef, RawListing

FIELD_LABELS = {
    "Marka pojazdu",
    "Model pojazdu",
    "Wersja",
    "Rok produkcji",
    "Rodzaj paliwa",
    "Skrzynia biegów",
    "Typ nadwozia",
    "Moc",
    "Pojemność skokowa",
    "Przebieg",
}


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def is_captcha_html(html: str) -> bool:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).lower()
    markers = ("captcha", "are you human", "potwierdź, że nie jesteś robotem")
    return any(marker in text for marker in markers)


def parse_listing_page(html: str, base_url: str = "https://www.otomoto.pl") -> list[ListingRef]:
    """Extract advert IDs and detail URLs from a search results page."""
    soup = BeautifulSoup(html, "html.parser")
    refs: list[ListingRef] = []
    seen: set[str] = set()
    for article in soup.select("article[data-id]"):
        advert_id = normalize_text(article.get("data-id"))
        link = article.select_one('a[href*="/oferta/"]') or article.select_one("h2 a[href]")
        if not advert_id or not isinstance(link, Tag) or not link.get("href"):
            continue
        if advert_id in seen:
            continue
        seen.add(advert_id)
        refs.append(ListingRef(advert_id=advert_id, url=urljoin(base_url, str(link["href"]))))
    return refs


def parse_total_pages(html: str) -> int:
    """Return the largest numeric pagination item, or 1 when absent."""
    soup = BeautifulSoup(html, "html.parser")
    values: list[int] = []
    for item in soup.select("ul li, nav li, a[href*='page=']"):
        text = normalize_text(item.get_text())
        if text.isdigit():
            values.append(int(text))
    return max(values) if values else 1


def _label_value_from_wrapper(wrapper: Tag) -> tuple[str, str] | None:
    paragraphs = wrapper.find_all("p")
    if len(paragraphs) < 2:
        return None
    label = normalize_text(paragraphs[0].get_text())
    value = normalize_text(paragraphs[1].get_text())
    if not label or not value:
        return None
    return label, value


def _extract_fields(soup: BeautifulSoup) -> dict[str, str]:
    fields: dict[str, str] = {}
    for label_element in soup.find_all("p", {"data-sentry-element": "Label"}):
        label = normalize_text(label_element.get_text())
        sibling = label_element.find_next_sibling("p")
        value = normalize_text(sibling.get_text()) if sibling else ""
        if label in FIELD_LABELS and value:
            fields[label] = value

    skip_testids = {
        "basic_information",
        "technical_specs",
        "condition_history",
        "financial_information",
        "collapsible-groups-wrapper",
    }
    for wrapper in soup.select("div[data-testid]"):
        testid = wrapper.get("data-testid")
        if testid in skip_testids:
            continue
        pair = _label_value_from_wrapper(wrapper)
        if pair is None:
            continue
        label, value = pair
        if label in FIELD_LABELS:
            fields[label] = value
    return fields


def _extract_price(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    price = soup.select_one(".offer-price__number")
    currency = soup.select_one(".offer-price__currency")
    if price is None:
        price = soup.select_one("[data-testid='ad-price-container'] span")
    return (
        normalize_text(price.get_text()) if price else None,
        normalize_text(currency.get_text()) if currency else None,
    )


def _extract_equipment(soup: BeautifulSoup) -> list[str]:
    section = soup.find("div", {"data-testid": "content-equipments-section"})
    if not section:
        return []
    items: list[str] = []
    for element in section.select("[data-sentry-element='EquipmentBox'], li, [data-testid] p"):
        text = normalize_text(element.get_text())
        if text and text not in items:
            items.append(text)
    return items


def _extract_advert_date(soup: BeautifulSoup) -> str | None:
    bottom = soup.find("div", {"data-sentry-element": "BottomWrapper"})
    if bottom:
        text = normalize_text(bottom.get_text(" "))
        match = re.search(r"(\d{1,2}\s+\w+\s+\d{4}(?:\s+\d{1,2}:\d{2})?)", text)
        if match:
            return match.group(1)
    date_element = soup.select_one("[data-testid='advert-date'], time")
    return normalize_text(date_element.get_text()) if date_element else None


def _extract_advert_id(soup: BeautifulSoup, fallback: str) -> str:
    text = normalize_text(soup.get_text(" "))
    match = re.search(r"\bID\s*:?\s*(\d{4,})\b", text)
    return match.group(1) if match else fallback


def parse_offer_page(
    html: str,
    source_url: str,
    advert_id: str | None = None,
    source: str = "otomoto",
    scraped_at: datetime | None = None,
) -> RawListing:
    """Parse one detail page into source-shaped fields."""
    soup = BeautifulSoup(html, "html.parser")
    fallback_id = source_url.rstrip("/").rsplit("/", 1)[-1]
    parsed_id = advert_id or _extract_advert_id(soup, fallback_id)
    price_raw, currency = _extract_price(soup)
    return RawListing(
        advert_id=parsed_id,
        source=source,
        source_url=source_url,
        scraped_at=scraped_at or datetime.now(timezone.utc),
        raw_fields=_extract_fields(soup),
        equipment=_extract_equipment(soup),
        price_raw=price_raw,
        currency=currency,
        advert_date_raw=_extract_advert_date(soup),
    )
