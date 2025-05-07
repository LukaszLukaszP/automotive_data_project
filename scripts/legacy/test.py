import os
import re
import time
import random
import csv
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup

from scraping_helpers import save_html_to_file, read_html_from_file

# Lista etykiet, które chcemy zachować w CSV (podstawowe informacje)
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

def rotate_user_agent(index=None):
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_0_1) AppleWebKit/535.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/535.36",
    ]
    if index is not None:
        return USER_AGENTS[index % len(USER_AGENTS)]
    return random.choice(USER_AGENTS)

def remove_cookie_banner(driver):
    """
    Usuwa / ukrywa bannery cookies i inne nakładki blokujące kliknięcia
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
    Klikamy przycisk 'Wyświetl VIN' (jeśli jest),
    aby odsłonić numer VIN.
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

def extract_vin(soup):
    """
    Wyszukuje VIN w <div data-testid="advert-vin">.
    """
    vin_container = soup.find("div", {"data-testid": "advert-vin"})
    if vin_container:
        vin_p = vin_container.find("p", {"data-sentry-element": "Label"})
        if vin_p:
            return vin_p.get_text(strip=True)
    return ""

def extract_offer_data(soup):
    """
    Wyodrębnia etykiety <p data-sentry-element="Label"> oraz ich wartości
    (następne rodzeństwo <p>) i filtruje je do listy REQUIRED_LABELS.
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
    Znajduje wszystkie elementy wyposażenia w sekcji "Wyposażenie" na podstawie atrybutu
    data-testid="content-equipments-section" i atrybutu data-sentry-element="EquipmentBox".
    Zbiera tekst z elementów <p data-sentry-element="Text"> i łączy je w jeden string, 
    oddzielając znakiem '|'.
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
    Wyszukuje cenę, walutę i poziom ceny (jeśli dostępny).
    Zwraca słownik z kluczami: price, currency, price_level.
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
    Pobiera datę i ID ogłoszenia wykorzystując strukturę DOM
    i atrybuty data-sentry-element. Funkcja szuka głównego kontenera
    BottomWrapper, a następnie zakłada, że:
      - Pierwszy obszar (Area) zawiera datę ogłoszenia.
      - Drugi obszar zawiera przycisk z ID ogłoszenia.
    Zwraca słownik z kluczami: advert_date, advert_id.
    """
    data = {}
    bottom_wrapper = soup.find("div", {"data-sentry-element": "BottomWrapper"})
    if bottom_wrapper:
        areas = bottom_wrapper.find_all("div", {"data-sentry-element": "Area"})
        # Data ogłoszenia
        if len(areas) >= 1:
            date_p = areas[0].find("p", {"data-sentry-element": "Text"})
            data["advert_date"] = date_p.get_text(strip=True) if date_p else ""
        else:
            data["advert_date"] = ""
        # ID ogłoszenia
        if len(areas) >= 2:
            button = areas[1].find("button")
            if button:
                id_p = button.find("p", {"data-sentry-element": "Text"})
                if id_p:
                    text = id_p.get_text(strip=True)
                    # Używamy wyrażenia regularnego, aby wyłuskać sam numer
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
    Znajduje sekcję z opisem na podstawie nagłówka <h2> z tekstem 'Opis'.
    Następnie wyszukuje w tym kontenerze elementy <p> i skleja ich tekst w jeden string.
    Zwraca pusty łańcuch, jeśli nie znajdzie opisu.
    """
    # 1. Szukamy nagłówka <h2> z tekstem "Opis"
    h2_opis = soup.find("h2", string="Opis")
    if not h2_opis:
        return ""  # jeśli nie ma takiego nagłówka, zwracamy pusty string
    
    # 2. Szukamy rodzica z data-testid="content-description-section"
    #    To powinno być główne opakowanie sekcji z opisem.
    description_container = h2_opis.find_parent("div", {"data-testid": "content-description-section"})
    if not description_container:
        return ""
    
    # 3. Wewnątrz kontenera szukamy <div data-testid="textWrapper">
    text_wrapper = description_container.find("div", {"data-testid": "textWrapper"})
    if not text_wrapper:
        return ""
    
    # 4. Wewnątrz textWrapper zwykle znajdują się <p> z właściwym opisem
    paragraphs = text_wrapper.find_all("p")
    
    # 5. Łączymy tekst z kolejnych <p>, np. wstawiając między nimi znak nowej linii
    description_text = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
    
    return description_text

def main():
    url = "https://www.otomoto.pl/osobowe/oferta/hyundai-tucson-hyundai-tucson-blue-1-6-crdi-2wd-select-ID6H7Lq4.html"
    
    options = Options()
    options.add_argument("--headless=new")
    
    with uc.Chrome(options=options) as driver:
        ua = rotate_user_agent(0)
        try:
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": ua})
        except Exception as e:
            print(f"[!] Error setting UA: {e}")
        
        driver.get(url)
        time.sleep(3)
        
        # Usuwamy overlay (baner cookies)
        remove_cookie_banner(driver)
        
        # Klikamy przycisk "Wyświetl VIN" (o ile jest)
        reveal_vin_if_available(driver)
        
        # Zapisujemy aktualny kod strony
        html_filename = "offer.html"
        save_html_to_file(driver, html_filename)
        print("HTML saved to file:", html_filename)
    
    # Po zamknięciu drivera wczytujemy zapisany HTML
    html_content = read_html_from_file(html_filename)
    soup = BeautifulSoup(html_content, "html.parser")
    
    # 1. Wyodrębniamy podstawowe dane (wg REQUIRED_LABELS)
    extracted_data = extract_offer_data(soup)
    
    # 2. Wyodrębniamy VIN
    vin = extract_vin(soup)
    if vin:
        extracted_data["VIN"] = vin
    
    # 3. Wyodrębniamy wyposażenie z poszczególnych sekcji
    #equipment_data = extract_equipment_sections(soup)
    #extracted_data.update(equipment_data)
    equipment_text = extract_all_equipment(soup)
    extracted_data["equipment"] = equipment_text
    
    # 4. Wyodrębniamy cenę, walutę i poziom ceny
    price_data = extract_price_info(soup)
    extracted_data.update(price_data)

    # 5. Data i ID ogłoszenia
    metadata = extract_advert_metadata(soup)
    extracted_data.update(metadata)

    opis = extract_description(soup)

    # Dodajemy opis jako kolejny klucz
    extracted_data["description"] = opis
    #extracted_data["description"] = opis.replace("\n", " ")

    print("\nExtracted Data:")
    for key, val in extracted_data.items():
        print(f"{key}: {val}")
    
    # Definiujemy kolumny CSV: podstawowe dane, VIN, wyposażenie, cena i metadata oraz opis
    fieldnames = (
        REQUIRED_LABELS
        + ["VIN"]
        #+ list(EQUIPMENT_SECTION_COLUMNS.values())
        + ["equipment"] 
        + ["price", "currency", "price_level", "advert_date", "advert_id", "description"]
    )
    
    csv_filename = "offer.csv"
    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        row_data = {col: extracted_data.get(col, "") for col in fieldnames}
        writer.writerow(row_data)
    
    print(f"\nDane zapisane do pliku: {csv_filename}")

if __name__ == "__main__":
    main()