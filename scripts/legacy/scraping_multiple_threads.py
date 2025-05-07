import os
import re
import time
import random
import csv
import requests
import unicodedata
from datetime import datetime
from bs4 import BeautifulSoup
import concurrent.futures

# -------------------------------------------------------------------
# Toggle test mode: True for limited scraping, False for full production scraping
# -------------------------------------------------------------------
TEST_MODE = True

# -------------------------------------------------------------------
# Mandatory fields to collect for each vehicle listing
# -------------------------------------------------------------------
REQUIRED_LABELS = [
    "Marka pojazdu", "Model pojazdu", "Wersja", "Kolor", "Liczba drzwi", "Liczba miejsc",
    "Rok produkcji", "Generacja", "Rodzaj paliwa", "Pojemność skokowa", "Moc",
    "Typ nadwozia", "Rodzaj koloru", "Skrzynia biegów", "Napęd", "Emisja CO2",
    "Spalanie W Mieście", "Spalanie Poza Miastem", "Kraj pochodzenia", "Przebieg",
    "Numer rejestracyjny pojazdu", "Stan", "Bezwypadkowy",
    "Data pierwszej rejestracji w historii pojazdu", "Zarejestrowany w Polsce",
    "Pierwszy właściciel (od nowości)", "Serwisowany w ASO", "Ma numer rejestracyjny",
    "Pojemność baterii", "Autonomia", "Średnie zużycie", "Kondycja baterii",
    "Typ złącza ładowania", "Elektryczna moc maksymalna HP", "Liczba silników",
    "Odzyskiwanie energii hamowania", "Liczba baterii"
]

# -------------------------------------------------------------------
# List of User-Agent strings for random rotation to avoid blocking
# -------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_0_1) AppleWebKit/535.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/535.36",
]


def rotate_user_agent():
    """Selects a random User-Agent header for each request."""
    return random.choice(USER_AGENTS)


def is_captcha_page(soup):
    """Detects if the response page contains a CAPTCHA or rate-limit message."""
    text = soup.get_text().lower()
    return "captcha" in text or "too many requests" in text

# -------------------------------------------------------------------
# HTTP request with retries and CAPTCHA detection
# -------------------------------------------------------------------
def get_soup(url, session, max_retries=3):
    """
    Fetches a URL and returns a BeautifulSoup object,
    retrying on failure or CAPTCHA detection up to max_retries.
    """
    for attempt in range(max_retries):
        try:
            headers = {"User-Agent": rotate_user_agent()}
            time.sleep(random.uniform(1.5, 3.5))  # polite crawl delay
            response = session.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                if is_captcha_page(soup):
                    print(f"[!] CAPTCHA encountered at {url}, pausing before retrying...")
                    time.sleep(60)
                    continue
                return soup
            else:
                print(f"[!] Received status {response.status_code} for {url}; retry {attempt+1}/{max_retries}")
                time.sleep(10)
        except Exception as e:
            print(f"[!] Error fetching {url}; retry {attempt+1}/{max_retries}: {e}")
            time.sleep(10)
    return None  # give up after retries

# -------------------------------------------------------------------
# Determine total number of result pages for a search URL
# -------------------------------------------------------------------
def get_total_pages(url, session):
    """
    Parses pagination controls to find the highest page number.
    Returns 1 if unable to detect multiple pages.
    """
    soup = get_soup(url, session)
    if not soup:
        return 1
    pagination = soup.find("ul", class_="ooa-1vdlgt7")
    if pagination:
        page_nums = [int(li.get_text(strip=True))
                     for li in pagination.find_all("li")
                     if li.get_text(strip=True).isdigit()]
        if page_nums:
            return max(page_nums)
    return 1

# -------------------------------------------------------------------
# Split a base search link into sub-links by year (and gearbox if needed)
# to stay under 500 pages per segment as required by the site.
# -------------------------------------------------------------------
def split_link(url, session):
    print("Checking total pages for base URL...")
    total = get_total_pages(url, session)
    if total <= 500:
        return [url]
    print(f"[!] Found {total} pages; splitting by production year ranges.")
    segments = []
    current_year = datetime.now().year
    for start_year in range(2000, current_year + 1, 1):
        end_year = start_year
        year_url = url.replace("osobowe", f"osobowe/od-{start_year}", 1)
        if "search%5Bfilter_float_year%3Ato%5D" not in year_url:
            year_url += f"&search%5Bfilter_float_year%3Ato%5D={end_year}"
        pages_for_range = get_total_pages(year_url, session)
        if pages_for_range <= 500:
            segments.append(year_url)
        else:
            for gearbox in ["manual", "automatic"]:
                segments.append(year_url + f"&search%5Bfilter_enum_gearbox%5D={gearbox}")
    return segments

# -------------------------------------------------------------------
# Extract all offer links from a single results page
# -------------------------------------------------------------------
def get_offer_links_from_page(url, session):
    links = []
    soup = get_soup(url, session)
    if not soup:
        return links
    for offer in soup.find_all("article", {"data-id": True}):
        a_tag = offer.select_one("h2 a")
        if a_tag and a_tag.has_attr("href"):
            links.append(a_tag["href"])
    return links

# -------------------------------------------------------------------\# Helpers for parsing individual listing pages
# -------------------------------------------------------------------
TESTID_TO_LABEL = {
    "registration":      "Numer rejestracyjny pojazdu",
    "date_registration": "Data pierwszej rejestracji w historii pojazdu",
}

def normalize(text: str) -> str:
    """Normalize whitespace and unicode characters."""
    text = unicodedata.normalize("NFKC", text).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()

def get_value_from_wrap(wrap):
    """
    Extracts the value text from a wrapper element, ignoring the label.
    """
    candidate = wrap.select_one("p.ed2m2uu0, p.ekwurce9")
    if candidate:
        return candidate.get_text(strip=True)
    paragraphs = wrap.find_all("p")
    if len(paragraphs) >= 2:
        label_text = paragraphs[0].get_text(strip=True)
        for p in paragraphs[1:]:
            text = p.get_text(strip=True)
            if text and text != label_text:
                return text
    return ""

# -------------------------------------------------------------------
# Parse the fields from a single offer page into a data dictionary
# -------------------------------------------------------------------
def extract_offer_data(soup):
    data = {}
    # 1. Legacy layout: label and sibling <p> structure
    for label_elem in soup.find_all("p", {"data-sentry-element": "Label"}):
        label = normalize(label_elem.get_text())
        if label in REQUIRED_LABELS:
            value_elem = label_elem.find_next_sibling("p")
            if value_elem and (label not in data or not data[label]):
                data[label] = value_elem.get_text(strip=True)
    # 2. New layout: elements with data-testid attributes
    skip_ids = {"basic_information", "technical_specs", "condition_history",
                "financial_information", "collapsible-groups-wrapper"}
    for wrap in soup.select("div[data-testid]"):
        tid = wrap["data-testid"]
        if tid in skip_ids:
            continue
        if tid in TESTID_TO_LABEL:
            val = get_value_from_wrap(wrap)
            label = TESTID_TO_LABEL[tid]
            if not data.get(label):
                data[label] = val
            continue
        paras = wrap.find_all("p", recursive=True)
        if len(paras) >= 2:
            label_txt = normalize(paras[0].get_text())
            if label_txt in REQUIRED_LABELS:
                data[label_txt] = get_value_from_wrap(wrap)
    # 3. Equipment list
    equipment_section = soup.find("div", {"data-testid": "content-equipments-section"})
    equipment = []
    if equipment_section:
        boxes = equipment_section.find_all("div", {"data-sentry-element": "EquipmentBox"}) or \
                equipment_section.select("div[data-testid] p")
        for box in boxes:
            text_elem = box.find("p", {"data-sentry-element": "Text"}) or box
            equipment.append(text_elem.get_text(strip=True))
    data["equipment"] = "|".join(equipment)
    # 4. Price, currency, and price indicator
    data["price"] = (soup.find("span", class_="offer-price__number") or "").get_text(strip=True)
    data["currency"] = (soup.find("span", class_="offer-price__currency") or "").get_text(strip=True)
    # Attempt to extract the price indicator label (e.g. "Below average", "W średniej", etc.)
    price_label = soup.find("p", attrs={"data-testid": "price-indicator-label-IN"})
    if not price_label:
        small = soup.find("div", attrs={"data-testid": "small-price-evaluation-indicator"})
        if small:
            price_label = next((p for p in small.find_all("p") if p.get_text(strip=True)), None)
    data["price_indicator"] = price_label.get_text(strip=True) if price_label else ""
    # 5. Listing publication date and ID
    bottom = soup.find("div", {"data-sentry-element": "BottomWrapper"})
    if bottom:
        areas = bottom.find_all("div", {"data-sentry-element": "Area"})
        if areas:
            date_elem = areas[0].find("p", {"data-sentry-element": "Text"})
            data["advert_date"] = date_elem.get_text(strip=True) if date_elem else ""
        if len(areas) > 1:
            id_elem = areas[1].find("p", {"data-sentry-element": "Text"})
            if id_elem:
                m = re.search(r"ID\s*:\s*(\d+)", id_elem.get_text(strip=True))
                data["advert_id"] = m.group(1) if m else id_elem.get_text(strip=True)
    # 6. Text description
    desc_paragraphs = []
    header = soup.find("h2", string="Opis")
    if header:
        container = header.find_parent("div", {"data-testid": "content-description-section"})
        if container:
            wrapper = container.find("div", {"data-testid": "textWrapper"})
            if wrapper:
                desc_paragraphs = [p.get_text(strip=True) for p in wrapper.find_all("p") if p.get_text(strip=True)]
    data["description"] = "\n".join(desc_paragraphs)
    # Debug output for key fields
    print(data.get("Numer rejestracyjny pojazdu"), data.get("Data pierwszej rejestracji w historii pojazdu"), data.get("price_indicator"))
    return data

# -------------------------------------------------------------------
# Process a split URL: scrape up to TEST_MODE pages and write to CSV
# -------------------------------------------------------------------
def process_splitted_link(idx, url, session):
    all_offers = []
    total_pages = get_total_pages(url, session)
    print(f"[Segment {idx}] Processing {url} with {total_pages} pages")
    pages_to_scrape = 1 if TEST_MODE else min(total_pages, 500)
    for page in range(1, pages_to_scrape + 1):
        page_url = f"{url}&page={page}"
        print(f"  Fetching page: {page_url}")
        links = get_offer_links_from_page(page_url, session)
        if not links:
            print(f"  No listings found on page {page}")
            continue
        for offer_url in links:
            time.sleep(random.uniform(2, 4))
            soup_offer = get_soup(offer_url, session)
            if not soup_offer:
                print(f"    Failed to retrieve listing: {offer_url}")
                continue
            record = extract_offer_data(soup_offer)
            all_offers.append(record)
        time.sleep(random.uniform(3, 6))
    if all_offers:
        headers = REQUIRED_LABELS + ["equipment", "price", "currency", "price_indicator", "advert_date", "advert_id", "description"]
        filename = (os.path.join("data", f"test_offers_segment_{idx}.csv") if TEST_MODE else
                    os.path.join("data", f"offers_segment_{idx}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"))
        os.makedirs("data", exist_ok=True)
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in all_offers:
                writer.writerow({col: row.get(col, '') for col in headers})
        print(f"✅ Saved segment {idx} data to {filename}\n")
    else:
        print(f"No data extracted for segment {idx}.\n")
    return idx

# -------------------------------------------------------------------
# Main execution: split link and process each segment concurrently
# -------------------------------------------------------------------
def main():
    session = requests.Session()
    base_url = (
        "https://www.otomoto.pl/osobowe?"
        "search%5Bfilter_enum_damaged%5D=0&"
        "search%5Badvanced_search_expanded%5D=true"
    )
    print(f"Starting URL segmentation for base link: {base_url}")
    segments = split_link(base_url, session)
    print(f"Generated {len(segments)} segment(s) for scraping.")
    if TEST_MODE:
        segments = segments[:1]  # limit to first segment in test mode
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_splitted_link, i, url, session)
                   for i, url in enumerate(segments, start=1)]
        for future in concurrent.futures.as_completed(futures):
            try:
                idx = future.result()
                print(f"Completed segment {idx}")
            except Exception as err:
                print(f"Error in segment processing: {err}")

if __name__ == "__main__":
    main()
