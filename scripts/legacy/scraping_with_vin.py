import os
import re
import time
import random
import csv
import psutil
import undetected_chromedriver as uc
from datetime import datetime
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from scripts.scraping_helpers import save_html_to_file, read_html_from_file

# -------------------------------------------------------------------
# Set test mode (True = one page per link, False = full download)
# -------------------------------------------------------------------
TEST_MODE = True

# -------------------------------------------------------------------
# REQUIRED FIELDS (basic information)
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
# HELPER FUNCTIONS (User-Agent rotation, captcha check, wait for page load, etc.)
# -------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_0_1) AppleWebKit/535.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/535.36",
]

def rotate_user_agent(index=None):
    """
    Returns a random or cyclic User-Agent from the list.
    """
    if index is not None:
        return USER_AGENTS[index % len(USER_AGENTS)]
    return random.choice(USER_AGENTS)

def is_captcha_page(soup):
    """
    Checks if the page contains a captcha or a "too many requests" message.
    """
    text = soup.get_text().lower()
    return "captcha" in text or "too many requests" in text

def wait_for_page_load(driver, timeout=10):
    """
    Waits until an element representing an offer (an article with a data-id) is loaded.
    """
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "article[data-id]"))
    )

# -------------------------------------------------------------------
# HELPER FUNCTIONS TO SAVE AND READ HTML (optional)
# -------------------------------------------------------------------
def save_html_to_file(driver, filename):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(driver.page_source)

def read_html_from_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return f.read()
    
# -------------------------------------------------------------------
# FUNCTIONS FOR HANDLING THE LIST OF LINKS
# -------------------------------------------------------------------
def get_offer_links_from_page(url, driver):
    """
    Retrieves the offer links from a results page.
    """
    links = []
    try:
        driver.get(url)
        time.sleep(random.uniform(3, 6))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        offers = soup.find_all("article", {"data-id": True})
        for offer in offers:
            a_tag = offer.select_one("h2 a")
            if a_tag and a_tag.has_attr("href"):
                links.append(a_tag["href"])
    except Exception as e:
        print(f"[!] Error getting links from {url}: {e}")
    return links

def get_total_pages(url, driver):
    """
    Returns the total number of result pages for the given URL.
    """
    driver.get(url)
    time.sleep(random.uniform(3, 6))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    pagination = soup.find("ul", class_="ooa-1vdlgt7")
    if pagination:
        pages = [int(li.get_text(strip=True)) for li in pagination.find_all("li") if li.get_text(strip=True).isdigit()]
        if pages:
            return max(pages)
    return 1

def split_link(url, driver):
    """
    Splits the main URL into filtered URLs (e.g., by year range).
    If the total number of pages is too high (>500), applies further filters.
    """
    driver.get(url)
    time.sleep(random.uniform(2, 4))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    total_pages = 0
    pagination_ul = soup.find("ul", class_="ooa-1vdlgt7")
    if pagination_ul:
        page_numbers = [int(li.get_text(strip=True)) for li in pagination_ul.find_all("li") if li.get_text(strip=True).isdigit()]
        if page_numbers:
            total_pages = max(page_numbers)
    
    if total_pages <= 500:
        return [url]
    
    filtered_urls = []
    min_year = 2000
    max_year = datetime.now().year
    for start_year in range(min_year, max_year + 1, 2):
        end_year = start_year + 1
        new_url = url.replace("osobowe", f"osobowe/from-{start_year}", 1)
        if "search%5Bfilter_float_year%3Ato%5D" not in new_url:
            new_url += f"&search%5Bfilter_float_year%3Ato%5D={end_year}"
        
        driver.get(new_url)
        time.sleep(random.uniform(1, 3))
        soup_new = BeautifulSoup(driver.page_source, "html.parser")
        total_pages_new = 0
        pagination_ul_new = soup_new.find("ul", class_="ooa-1vdlgt7")
        if pagination_ul_new:
            page_numbers_new = [int(li.get_text(strip=True)) for li in pagination_ul_new.find_all("li") if li.get_text(strip=True).isdigit()]
            if page_numbers_new:
                total_pages_new = max(page_numbers_new)
        
        if total_pages_new <= 500:
            filtered_urls.append(new_url)
        else:
            # Further filter by gearbox type if still too many pages
            for gearbox in ["manual", "automatic"]:
                gearbox_url = new_url + f"&search%5Bfilter_enum_gearbox%5D={gearbox}"
                driver.get(gearbox_url)
                time.sleep(random.uniform(1, 3))
                soup_gear = BeautifulSoup(driver.page_source, "html.parser")
                total_pages_gear = 0
                pagination_ul_gear = soup_gear.find("ul", class_="ooa-1vdlgt7")
                if pagination_ul_gear:
                    page_numbers_gear = [int(li.get_text(strip=True)) for li in pagination_ul_gear.find_all("li") if li.get_text(strip=True).isdigit()]
                    if page_numbers_gear:
                        total_pages_gear = max(page_numbers_gear)
                filtered_urls.append(gearbox_url)
    return filtered_urls

# -------------------------------------------------------------------
# FUNCTIONS TO REMOVE COOKIE BANNERS AND REVEAL THE VIN
# -------------------------------------------------------------------
def remove_cookie_banner(driver):
    """
    Removes/hides cookie banners and other overlays that may block click actions.
    """
    try:
        driver.execute_script(
            "document.querySelectorAll('#onetrust-policy, .onetrust-pc-dark-filter').forEach(e => e.remove());"
        )
        time.sleep(0.5)
    except Exception as e:
        print(f"Błąd przy usuwaniu overlay: {e}") 

def reveal_vin_if_available(driver):
    """
    Clicks the "Wyświetl VIN" button (if available) to reveal the VIN number.
    """
    try:
        vin_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Wyświetl VIN')]")
        driver.execute_script("arguments[0].scrollIntoView(true);", vin_button)
        driver.execute_script("arguments[0].click();", vin_button)
        time.sleep(1)
    except NoSuchElementException:
        print("Nie znaleziono przycisku 'Wyświetl VIN'.")
    except Exception as e:
        print(f"Błąd przy próbie kliknięcia w 'Wyświetl VIN': {e}")

# -------------------------------------------------------------------
# FUNCTIONS FOR EXTRACTING DATA FROM THE OFFER PAGE
# -------------------------------------------------------------------
def extract_vin(soup):
    """
    Searches for the VIN within the <div data-testid="advert-vin"> element.
    """
    vin_container = soup.find("div", {"data-testid": "advert-vin"})
    if vin_container:
        vin_p = vin_container.find("p", {"data-sentry-element": "Label"})
        if vin_p:
            return vin_p.get_text(strip=True)
    return ""

def extract_offer_data(soup):
    """
    Extracts the <p data-sentry-element="Label"> elements and their subsequent sibling <p> values,
    filtering them based on the REQUIRED_LABELS list.
    """
    data = {}
    all_labels = soup.find_all("p", {"data-sentry-element": "Label"})
    for label_elem in all_labels:
        label_text = label_elem.get_text(strip=True)
        if label_text not in REQUIRED_LABELS:
            continue
        value_elem = label_elem.find_next_sibling("p")
        if value_elem:
            data[label_text] = value_elem.get_text(strip=True)
    return data

def extract_all_equipment(soup):
    """
    Finds all equipment items in the "Wyposażenie" (Equipment) section by using the
    data-testid "content-equipments-section" and data-sentry-element "EquipmentBox".
    Collects the text from elements <p data-sentry-element="Text"> and joins them into a single string separated by '|'.
    """
    container = soup.find("div", {"data-testid": "content-equipments-section"})
    if not container:
        return ""
    
    equipment_boxes = container.find_all("div", {"data-sentry-element": "EquipmentBox"})
    items = []
    for box in equipment_boxes:
        p_elem = box.find("p", {"data-sentry-element": "Text"})
        if p_elem:
            items.append(p_elem.get_text(strip=True))
    return "|".join(items)

def extract_price_info(soup):
    """
    Searches for the price, currency, and price level (if available).
    Returns a dictionary with the keys: price, currency, price_level.
    """
    data = {}
    
    price_number = soup.find("span", class_="offer-price__number")
    data["price"] = price_number.get_text(strip=True) if price_number else ""
    
    currency_span = soup.find("span", class_="offer-price__currency")
    data["currency"] = currency_span.get_text(strip=True) if currency_span else ""
    
    price_indicator_label = soup.find("p", {"data-testid": "price-indicator-label-IN"})
    data["price_level"] = price_indicator_label.get_text(strip=True) if price_indicator_label else ""
    
    return data

def extract_advert_metadata(soup):
    """
    Retrieves the advertisement date and ID using the DOM structure and data-sentry-element attributes.
    The function looks for the main container "BottomWrapper", then assumes that:
      - The first area (Area) contains the advertisement date.
      - The second area contains a button with the advertisement ID.
    Returns a dictionary with the keys: advert_date, advert_id.
    """
    data = {}
    bottom_wrapper = soup.find("div", {"data-sentry-element": "BottomWrapper"})
    if bottom_wrapper:
        areas = bottom_wrapper.find_all("div", {"data-sentry-element": "Area"})
        # Advertisement date
        if len(areas) >= 1:
            date_p = areas[0].find("p", {"data-sentry-element": "Text"})
            data["advert_date"] = date_p.get_text(strip=True) if date_p else ""
        else:
            data["advert_date"] = ""
        # Advertisement ID
        if len(areas) >= 2:
            button = areas[1].find("button")
            if button:
                id_p = button.find("p", {"data-sentry-element": "Text"})
                if id_p:
                    text = id_p.get_text(strip=True)
                    # Using a regular expression to extract just the number
                    match = re.search(r'ID\s*:\s*(\d+)', text)
                    if match:
                        data["advert_id"] = match.group(1)
                    else:
                        data["advert_id"] = text
                else:
                    data["advert_id"] = ""
            else:
                data["advert_id"] = ""
        else:
            data["advert_id"] = ""
    else:
        data["advert_date"] = ""
        data["advert_id"] = ""
    return data

def extract_description(soup):
    """
    Finds the description section based on an <h2> header with the text "Opis".
    Then it searches within that container for <p> elements and concatenates their text into one string.
    Returns an empty string if no description is found.
    """
    h2_opis = soup.find("h2", string="Opis")
    if not h2_opis:
        return "" 
    description_container = h2_opis.find_parent("div", {"data-testid": "content-description-section"})
    if not description_container:
        return ""
    text_wrapper = description_container.find("div", {"data-testid": "textWrapper"})
    if not text_wrapper:
        return ""
    paragraphs = text_wrapper.find_all("p")    
    description_text = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
    return description_text

def scrape_offer_data(driver, url, index=0):
    """
    Opens the offer page, removes cookie banners, reveals the VIN, saves the HTML,
    and extracts data based on predefined rules.
    """
    data = {}
    try:
        # Set the User-Agent for each offer (optional)
        ua = rotate_user_agent(index)
        try:
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": ua})
        except Exception as e:
            print(f"[!] Błąd przy ustawianiu UA: {e}")

        driver.get(url)
        time.sleep(3)  # Waiting for the page to load
        remove_cookie_banner(driver)
        reveal_vin_if_available(driver)

        # Save HTML to file (optionally, you can skip this and parse directly from driver.page_source)
        temp_filename = f"offer_{index}.html"
        save_html_to_file(driver, temp_filename)
        html_content = read_html_from_file(temp_filename)
        try:
            os.remove(temp_filename)
        except:
            pass

        soup = BeautifulSoup(html_content, "html.parser")

        # Collect data
        extracted_data = extract_offer_data(soup)
        vin = extract_vin(soup)
        if vin:
            extracted_data["VIN"] = vin
        extracted_data["equipment"] = extract_all_equipment(soup)
        price_data = extract_price_info(soup)
        extracted_data.update(price_data)
        metadata = extract_advert_metadata(soup)
        extracted_data.update(metadata)
        opis = extract_description(soup)
        extracted_data["description"] = opis

        data = extracted_data
    except Exception as e:
        print(f"[!] Błąd przy zbieraniu danych z oferty {url}: {e}")
    return data

# -------------------------------------------------------------------
# MAIN FUNCTION TO COLLECT ALL OFFERS
# -------------------------------------------------------------------
def main():
    # Define a list for all collected data
    all_offers_data = []
    # Set to avoid duplicates (e.g., using advert_id)
    seen_offers = set()

    base_url = "https://www.otomoto.pl/osobowe?search%5Bfilter_enum_damaged%5D=0&search%5Badvanced_search_expanded%5D=true"

    print("Rozpoczynam dzielenie linków...")
    splitted_links = split_link(base_url, driver)
    print(f"Utworzono {len(splitted_links)} link(ów) po splitowaniu.")

    if TEST_MODE:
        # In test mode, limit to only the first link
        splitted_links = splitted_links[:1]

    # Iterating through the (potentially filtered) links
    for idx, filtered_url in enumerate(splitted_links):
        total_pages = get_total_pages(filtered_url, driver)
        print(f"⚙️ Link nr {idx+1}/{len(splitted_links)}: {filtered_url}")
        print(f"   Liczba znalezionych stron: {total_pages}")

        # In test mode, scrape only 1 page; otherwise, up to 500 pages
        pages_to_scrape = 1 if TEST_MODE else min(total_pages, 500)

        for page_num in range(1, pages_to_scrape + 1):
            page_url = f"{filtered_url}&page={page_num}"
            print(f"   -> Przetwarzam stronę: {page_url}")

            # Set the User-Agent for each page
            ua = rotate_user_agent(page_num)
            try:
                driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": ua})
            except Exception as e:
                print(f"[!] Błąd przy ustawianiu UA: {e}")

            retry_count = 0
            max_retries = 2
            offer_links = []

            # Attempt to retrieve links from the page (with several retries)
            while retry_count < max_retries:
                try:
                    driver.get(page_url)
                except Exception as e:
                    print(f"[!] Błąd przy wczytywaniu strony {page_url}: {e}")
                    retry_count += 1
                    time.sleep(10)
                    continue

                try:
                    wait_for_page_load(driver, timeout=10)
                except Exception as e:
                    print(f"[!] Timeout przy ładowaniu strony {page_url}: {e}")

                soup = BeautifulSoup(driver.page_source, "html.parser")
                if is_captcha_page(soup):
                    print("❗ CAPTCHa wykryta. Oczekuję 90 sek i ponawiam próbę.")
                    time.sleep(90)
                    retry_count += 1
                    continue

                offer_links = get_offer_links_from_page(page_url, driver)
                if not offer_links:
                    print(f"⚠️ Brak ofert na stronie {page_url}. Czekam 15 sekund i ponawiam próbę.")
                    time.sleep(15)
                    retry_count += 1
                    continue
                else:
                    # Successfully retrieved links, exit the retry loop
                    break

            if not offer_links:
                print(f"⚠️ Rezygnuję z strony {page_url} po {max_retries} nieudanych próbach.")
                continue

            # Retrieve data for each offer on the page
            for offer_index, offer_url in enumerate(offer_links):
                print(f"      ➜ Oferta: {offer_url}")
                offer_data = scrape_offer_data(driver, offer_url, offer_index)

                # Check the advertisement ID to avoid storing duplicates
                advert_id = offer_data.get("advert_id", "") or offer_url
                if advert_id in seen_offers:
                    print("      🔄 Duplikat – pomijam.")
                    continue

                seen_offers.add(advert_id)
                all_offers_data.append(offer_data)

                # Short pause between offers
                time.sleep(random.uniform(2, 4))

            # (Optional) Pause between pages
            time.sleep(random.uniform(3, 6))

        # (Optional) Pause between links
        time.sleep(random.uniform(3, 6))

    # After collecting all offers, save the data to CSV
    if all_offers_data:
        # Define the CSV headers
        fieldnames = (
            REQUIRED_LABELS
            + ["VIN", "equipment", "price", "currency", "price_level", "advert_date", "advert_id", "description"]
        )

        csv_filename = "all_offers_otomoto.csv"
        with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row_data in all_offers_data:
                row = {col: row_data.get(col, "") for col in fieldnames}
                writer.writerow(row)

        print(f"\n✅ Zakończono! Zebrane dane zapisano do pliku: {csv_filename}")
    else:
        print("❌ Brak danych do zapisania.")

# -------------------------------------------------------------------
# SELENIUM CONFIGURATION AND EXECUTION
# -------------------------------------------------------------------
if __name__ == "__main__":
    from selenium_stealth import stealth

    options = Options()
    # Chrome 109+ headless mode: use "--headless=new"
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = uc.Chrome(options=options)
    # Hide the webdriver attribute
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    stealth(
        driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    try:
        main()
    finally:
        # Safely close the driver
        try:
            driver.quit()
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Błąd przy zamykaniu Selenium: {e}")
        finally:
            # Terminate any leftover "chrome" processes in the system
            for proc in psutil.process_iter():
                if "chrome" in proc.name().lower():
                    try:
                        proc.kill()
                    except psutil.NoSuchProcess:
                        pass
        print("✅ Skrypt zakończony.")