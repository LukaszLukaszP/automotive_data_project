# -*- coding: utf-8 -*-
import os
import csv
import threading
import re
import time
import random
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import concurrent.futures

# -------------------------------------------------------------------
# Set test mode (True=test, False=full scraping)
# -------------------------------------------------------------------
TEST_MODE = True

# -------------------------------------------------------------------
# List of mandatory fields (basic information)
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
# Random User-Agents
# -------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_0_1) AppleWebKit/535.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/535.36",
]

def rotate_user_agent():
    """Returns a random User-Agent from the list."""
    return random.choice(USER_AGENTS)


def is_captcha_page(soup):
    """Checks if the page contains a captcha or a 'too many requests' message."""
    text = soup.get_text().lower()
    return "captcha" in text or "too many requests" in text


def load_processed_ids(path="data/ids.csv"):
    ids = set()
    if os.path.isfile(path):
        with open(path, newline='', encoding="utf-8") as f:
            for row in csv.reader(f):
                if row:
                    ids.add(row[0].strip())
    return ids

PROCESSED_IDS = load_processed_ids()          ### NEW – global set of processed IDs
ID_LOCK = threading.Lock()                    ### NEW – for synchronization between threads

# -------------------------------------------------------------------
# Function to fetch page content with retry and captcha detection
# -------------------------------------------------------------------
def get_soup(url, session, max_retries=3):
    """
    Downloads the page at the given URL and returns a BeautifulSoup object
    or None in case of repeated failures.
    """
    for attempt in range(max_retries):
        try:
            # Set a random User-Agent and wait a short random time
            headers = {"User-Agent": rotate_user_agent()}
            time.sleep(random.uniform(1.5, 3.5))

            response = session.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                # Check for captcha or rate-limiting
                if is_captcha_page(soup):
                    print(f"[!] Captcha detected at {url}. Waiting and retrying..." )
                    time.sleep(60)
                    continue
                return soup
            else:
                print(f"[!] Error status code={response.status_code} for {url}, attempt {attempt+1}/{max_retries}")
                time.sleep(10)
        except Exception as e:
            print(f"[!] Exception while fetching {url}, attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(10)
    return None  # failed to fetch the page

# -------------------------------------------------------------------
# Functions for pagination and splitting links
# -------------------------------------------------------------------
def get_total_pages(url, session):
    """
    Returns the total number of result pages for the given URL.
    """
    soup = get_soup(url, session)
    if not soup:
        return 1

    pagination = soup.find("ul", class_="ooa-1vdlgt7")
    if pagination:
        page_nums = []
        for li in pagination.find_all("li"):
            txt = li.get_text(strip=True)
            if txt.isdigit():
                page_nums.append(int(txt))
        if page_nums:
            return max(page_nums)
    return 1


def split_link(url, session):
    """
    If the total page count exceeds 500, the link is split by production year
    and, if necessary, by gearbox type to stay under the 500-page limit.
    Returns a list of URLs that cover the entire range.
    """
    print("Checking the total number of pages for the main link...")
    total = get_total_pages(url, session)
    if total <= 500:
        return [url]

    print(f"[!] The link generates {total} pages. Splitting by production year...")
    filtered_urls = []
    min_year = 2000
    max_year = datetime.now().year

    for start_year in range(min_year, max_year + 1, 1):
        end_year = start_year

        # Create a new URL filtered by the year range
        new_url = url.replace("osobowe", f"osobowe/od-{start_year}", 1)
        if "search%5Bfilter_float_year%3Ato%5D" not in new_url:
            new_url += f"&search%5Bfilter_float_year%3Ato%5D={end_year}"

        # Check how many pages the new URL generates
        pages_new = get_total_pages(new_url, session)
        if pages_new <= 500:
            filtered_urls.append(new_url)
        else:
            # Further split by gearbox if still over 500 pages
            for gearbox in ["manual", "automatic"]:
                gearbox_url = new_url + f"&search%5Bfilter_enum_gearbox%5D={gearbox}"
                pages_gear = get_total_pages(gearbox_url, session)
                filtered_urls.append(gearbox_url)
    return filtered_urls


def get_offer_links_from_page(url, session, processed_ids):
    result = []
    soup = get_soup(url, session)
    if not soup:
        return result

    offers = soup.find_all("article", {"data-id": True})
    for offer in offers:
        advert_id = offer["data-id"]
        with ID_LOCK:                # protect access across threads
            if advert_id in processed_ids:
                continue
        a_tag = offer.select_one("h2 a")
        if a_tag and a_tag.has_attr("href"):
            result.append((advert_id, a_tag["href"]))
    return result

# -------------------------------------------------------------------
# Functions to extract information from a single offer
# -------------------------------------------------------------------
def extract_offer_data(soup):
    """
    Extracts relevant data from an offer page, including:
    - Basic labels and values  (old layout first, then fallback to new layout)
    - Equipment
    - Price (price, currency, price_level)
    - Publication date, offer ID
    - Description
    """
    data = {}

    # ----------------------------------------------------------------
    # 1. BASIC INFORMATION (old layout first, then fallback to new layout)
    # ----------------------------------------------------------------
    extracted_any = False          # helps detect if old layout worked

    # ---- 1a. OLD layout (data-sentry-element="Label") ------------
    for label_elem in soup.find_all("p", {"data-sentry-element": "Label"}):
        label_text = label_elem.get_text(strip=True)
        if label_text in REQUIRED_LABELS:
            value_elem = label_elem.find_next_sibling("p")
            if value_elem:
                data[label_text] = value_elem.get_text(strip=True)
                extracted_any = True

    # ---- 1b. NEW layout (data-testid fields) -----------------------
    if not extracted_any:          # only if old layout returned nothing
        # skip general containers – look for individual fields
        SKIP_TESTIDS = {
            "basic_information", "technical_specs", "condition_history",
            "financial_information", "collapsible-groups-wrapper"
        }
        for wrapper in soup.select('div[data-testid]'):
            testid = wrapper.get('data-testid')
            if testid in SKIP_TESTIDS:
                continue

            # first <p> is label, second <p> is value
            paragraphs = wrapper.find_all("p", recursive=True, limit=2)
            if len(paragraphs) < 2:
                continue

            label_text = paragraphs[0].get_text(strip=True)
            value_text = paragraphs[1].get_text(strip=True)

            if label_text in REQUIRED_LABELS:
                data[label_text] = value_text

    # ----------------------------------------------------------------
    # 2. EQUIPMENT
    # ----------------------------------------------------------------
    equipment_section = soup.find("div", {"data-testid": "content-equipments-section"})
    equipment_list = []
    if equipment_section:
        eq_boxes = equipment_section.find_all("div", {"data-sentry-element": "EquipmentBox"})
        if not eq_boxes:                           # fallback in case attribute changed
            eq_boxes = equipment_section.select("div[data-testid] p")
        for box in eq_boxes:
            txt = box.find("p", {"data-sentry-element": "Text"}) or box
            if txt:
                equipment_list.append(txt.get_text(strip=True))
    data["equipment"] = "|".join(equipment_list)

    # ----------------------------------------------------------------
    # 3. PRICE
    # ----------------------------------------------------------------
    price_span = soup.find("span", class_="offer-price__number")
    currency_span = soup.find("span", class_="offer-price__currency")
    indicator = soup.find("p", {"data-testid": "price-indicator-label-IN"})

    data["price"]        = price_span.get_text(strip=True) if price_span else ""
    data["currency"]     = currency_span.get_text(strip=True) if currency_span else ""
    data["price_level"]  = indicator.get_text(strip=True)  if indicator    else ""

    # ----------------------------------------------------------------
    # 4. ADVERT DATE & ID
    # ----------------------------------------------------------------
    bottom_wrapper = soup.find("div", {"data-sentry-element": "BottomWrapper"})
    if bottom_wrapper:
        areas = bottom_wrapper.find_all("div", {"data-sentry-element": "Area"})
        # publication date
        if len(areas) >= 1:
            date_p = areas[0].find("p", {"data-sentry-element": "Text"})
            data["advert_date"] = date_p.get_text(strip=True) if date_p else ""
        else:
            data["advert_date"] = ""
        # advertisement ID
        if len(areas) >= 2:
            btn = areas[1].find("button")
            if btn:
                id_p = btn.find("p", {"data-sentry-element": "Text"})
                if id_p:
                    txt = id_p.get_text(strip=True)
                    match = re.search(r'ID\s*:\s*(\d+)', txt)
                    data["advert_id"] = match.group(1) if match else txt
                else:
                    data["advert_id"] = ""
            else:
                data["advert_id"] = ""
        else:
            data["advert_id"]   = ""
    else:
        data["advert_date"] = ""
        data["advert_id"]   = ""

    # ----------------------------------------------------------------
    # 5. DESCRIPTION
    # ----------------------------------------------------------------
    description = ""
    h2_opis = soup.find("h2", string="Opis")
    if h2_opis:
        desc_container = h2_opis.find_parent("div", {"data-testid": "content-description-section"})
        if desc_container:
            text_wrapper = desc_container.find("div", {"data-testid": "textWrapper"})
            if text_wrapper:
                paragraphs = text_wrapper.find_all("p")
                description = "\n".join(
                    p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
                )
    data["description"] = description

    return data


def process_splitted_link(idx: int, url: str, session: requests.Session) -> int:
    """
    Processes one of the split links:
      • iterates through result pages,
      • fetches only new offers,
      • writes data to CSV,
      • appends new advert IDs to data/ids.csv.
    Returns the link index (idx) – convenient for logging.
    """
    all_offers_data = []     # data to write to the offers CSV file
    new_ids = []             # IDs to append to ids.csv

    total_pages = get_total_pages(url, session)
    print(f"[{idx}] → {url} | pages: {total_pages}")

    pages_to_scrape = 1 if TEST_MODE else min(total_pages, 500)

    for page_num in range(1, pages_to_scrape + 1):
        page_url = f"{url}&page={page_num}"
        print(f"   Fetching list page: {page_url}")

        offers = get_offer_links_from_page(page_url, session, PROCESSED_IDS)
        if not offers:
            print("   No NEW offers on this page.")
            continue

        for advert_id, offer_url in offers:
            time.sleep(random.uniform(2, 4))

            soup_offer = get_soup(offer_url, session)
            if not soup_offer:
                print(f"      [!] Could not fetch offer: {offer_url}")
                continue

            extracted = extract_offer_data(soup_offer)
            all_offers_data.append(extracted)

            # remember ID in memory and add to list for saving
            with ID_LOCK:
                PROCESSED_IDS.add(advert_id)
            new_ids.append(advert_id)

        time.sleep(random.uniform(3, 6))

    # ------------------------------------------------------------------
    # 1. Saving offer data
    # ------------------------------------------------------------------
    if all_offers_data:
        fieldnames = (
            REQUIRED_LABELS
            + ["equipment", "price", "currency", "price_level",
               "advert_date", "advert_id", "description"]
        )
        out_csv = os.path.join("data", f"all_offers_otomoto_no_vin_{idx}.csv")
        os.makedirs("data", exist_ok=True)

        print(f"\nSaving offers to {out_csv}")
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in all_offers_data:
                writer.writerow({col: row.get(col, "") for col in fieldnames})
        print(f"✅ Done for link #{idx}\n")
    else:
        print(f"No new data for link #{idx}, nothing saved.\n")

    # ------------------------------------------------------------------
    # 2. Appending new IDs to ids.csv
    # ------------------------------------------------------------------
    if new_ids:
        ids_path = os.path.join("data", "ids.csv")
        with ID_LOCK:
            with open(ids_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                for _id in new_ids:
                    writer.writerow([_id])
        print(f"   ↳ Added {len(new_ids)} new advert_id to ids.csv")

    return idx

# -------------------------------------------------------------------
# Main function that collects offers
# -------------------------------------------------------------------
def main():
    # Create session and split the base link
    session = requests.Session()
    base_url = "https://www.otomoto.pl/osobowe?search%5Bfilter_enum_damaged%5D=0&search%5Badvanced_search_expanded%5D=true"
    print(f"Starting link splitting for: {base_url}")
    splitted_urls = split_link(base_url, session)
    print(f"Split into {len(splitted_urls)} link(s).")
    if TEST_MODE:
        splitted_urls = splitted_urls[:1]
    # Ensure that the target directory exists
    os.makedirs("data", exist_ok=True)

    # Use ThreadPoolExecutor to process each split link concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_splitted_link, idx, url, session) 
                   for idx, url in enumerate(splitted_urls, start=1)]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                print(f"Finished processing link #{result}.")
            except Exception as e:
                print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()