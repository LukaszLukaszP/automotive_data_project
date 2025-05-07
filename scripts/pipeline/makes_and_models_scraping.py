from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import csv
import urllib.parse
import sys

# Dictionary mapping custom brand names to URL slugs
custom_brand_slugs = {
    "BMW-ALPINA": "alpina",
    "Citroën": "citroen",
    "Doosan": "doosan",
    "e.GO": "e-go",
    "Lynk & Co": "lynk-and-co",
    "Warszawa": "marka_warszawa",
    "Wołga": "wolga",
    "Zaporożec": "zaporozec",
    "Zastava": "zastava",
}

# Function to generate a URL-friendly slug from a brand name
def create_slug(brand, mapping):
    if brand in mapping:
        return mapping[brand]
    else:
        brand_slug = brand.lower().replace(" ", "-")
        brand_slug = urllib.parse.quote(brand_slug)  # URL-encode special characters
        return brand_slug

# Function to navigate to a brand's page and extract model data with retry logic
def process_brand(driver, brand, base_url, csv_writer, mapping, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            brand_slug = create_slug(brand, mapping)
            brand_url = base_url.format(make_slug=brand_slug)
            print(f"\nNavigating to page for brand: {brand} -> {brand_url}")

            # Open the brand's page
            driver.get(brand_url)
            print(f"Page loaded for brand: {brand}")

            # Optional: wait a few seconds to allow the page to fully load
            time.sleep(3)

            # Verify that the page loaded correctly by checking the title
            page_title = driver.title
            print(f"Page title: {page_title}")

            # Expand the model filter section
            models_filter_container = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div[data-testid='filter_enum_model']"))
            )
            print("Model filter container located.")

            # Locate the button that expands the models list
            models_dropdown_button = models_filter_container.find_element(By.CSS_SELECTOR, "button[data-testid='arrow']")
            actions = ActionChains(driver)
            actions.move_to_element(models_dropdown_button).perform()

            # Click the dropdown button to reveal the models list
            models_dropdown_button.click()
            print("Clicked the button to expand the models list.")
            time.sleep(2)  # Allow time for the list to expand

            # Wait until the models list becomes visible
            models_list_container = WebDriverWait(driver, 20).until(
                EC.visibility_of_element_located((By.XPATH, "//div[@data-testid='filter_enum_model']//ul"))
            )
            print("Models list is now visible.")

            # Retrieve all items from the models list
            models_list_items = models_list_container.find_elements(By.TAG_NAME, "li")
            print(f"Found {len(models_list_items)} models for brand {brand}.")

            # Iterate through each model and write the brand-model pair to the CSV file
            for model_item in models_list_items:
                try:
                    model_p = model_item.find_element(By.TAG_NAME, "p")
                    model_text = model_p.text.strip()
                    model_name = model_text.split(" (")[0]  # Extract the model name

                    # Skip the option if it is "All Models"
                    if model_name.lower() in ["wszystkie modele", "all models"]:
                        print(f"Skipped option '{model_name}'.")
                        continue

                    csv_writer.writerow([brand, model_name])
                    print(f"Added to CSV: {brand} - {model_name}")
                except Exception as e:
                    print(f"Error extracting model name for brand {brand}: {e}")
                    driver.save_screenshot(f"error_model_name_{brand}.png")
                    continue  # Proceed to the next model

            # Optional: Deselect the brand if needed (logic can be added here)

            # Successfully processed the brand; exit the retry loop
            return

        except Exception as e:
            print(f"Error processing brand {brand}, attempt {attempt}/{max_retries}: {e}")
            driver.save_screenshot(f"error_processing_brand_{brand}_attempt{attempt}.png")
            if attempt == max_retries:
                print(f"Exceeded maximum retries for brand {brand}. Moving to the next brand.")
            else:
                print(f"Retrying brand {brand}...")
                time.sleep(5)  # Wait before retrying

# Initialize WebDriver options
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
# Optional: run in headless mode for background execution
# options.add_argument("--headless")

# Instantiate the WebDriver
driver = webdriver.Chrome(options=options)

# Define the main URL for the Otomoto page
main_url = "https://www.otomoto.pl/osobowe?search%5Border%5D=relevance_web&search%5Badvanced_search_expanded%5D=true"

try:
    # Open the main Otomoto page
    driver.get(main_url)
    print("Opened the main Otomoto page.")

    # Wait for and click the cookie acceptance button
    try:
        accept_cookies_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        accept_cookies_button.click()
        print("Accepted cookies.")
    except Exception as e:
        print(f"Cookie acceptance button not found or not clickable: {e}")
        # It might be that cookies are already accepted or not displayed

    # Wait for the brands filter container to be clickable
    try:
        brands_filter_container = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "div[data-testid='filter_enum_make']"))
        )
        print("Brands filter container located.")
    except Exception as e:
        print(f"Brands filter container not found: {e}")
        driver.save_screenshot("error_brands_filter_container.png")
        driver.quit()
        sys.exit()

    # Locate the dropdown button for the brands list
    try:
        brands_dropdown_button = brands_filter_container.find_element(By.CSS_SELECTOR, "button[data-testid='arrow']")
        # Optionally, scroll to the button using ActionChains
        actions = ActionChains(driver)
        actions.move_to_element(brands_dropdown_button).perform()

        # Click the dropdown button to reveal the brands list
        brands_dropdown_button.click()
        print("Clicked the button to expand the brands list.")
        time.sleep(2)  # Allow time for the list to expand
    except Exception as e:
        print(f"Unable to click the brands dropdown button: {e}")
        driver.save_screenshot("error_click_brands_dropdown.png")
        driver.quit()
        sys.exit()

    # Wait for the brands list to become visible
    try:
        brands_list_container = WebDriverWait(driver, 15).until(
            EC.visibility_of_element_located((By.XPATH, "//div[@data-testid='filter_enum_make']//ul"))
        )
        print("Brands list is now visible.")
    except Exception as e:
        print(f"Brands list did not appear: {e}")
        # For diagnostic purposes, print the inner HTML of the brands container
        try:
            brands_list_container = driver.find_element(By.CSS_SELECTOR, "div[data-testid='filter_enum_make']")
            print("Brands filter container HTML content:")
            print(brands_list_container.get_attribute('innerHTML'))
        except Exception as inner_e:
            print(f"Unable to retrieve container HTML: {inner_e}")
        driver.save_screenshot("error_brands_list_visible.png")
        driver.quit()
        sys.exit()

    # Retrieve all brand items from the list
    try:
        brands_list_items = brands_list_container.find_elements(By.TAG_NAME, "li")
        print(f"Found {len(brands_list_items)} brands.")
    except Exception as e:
        print(f"Error fetching brands list: {e}")
        driver.save_screenshot("error_fetch_brands_list.png")
        driver.quit()
        sys.exit()

    # Collect brand names into a list for further processing
    brand_names = []
    for index, brand_item in enumerate(brands_list_items, start=1):
        try:
            # Locate the text element containing the brand name
            brand_p = brand_item.find_element(By.TAG_NAME, "p")
            brand_text = brand_p.text.strip()
            brand_name = brand_text.split(" (")[0]  # Extract the brand name

            # Skip the option if it's "All Brands" (typically the first element)
            if index == 1 and brand_name.lower() in ["wszystkie marki", "all makes"]:
                print(f"Skipped option '{brand_name}'.")
                continue

            brand_names.append(brand_name)
        except Exception as e:
            print(f"Error processing brand at index {index}: {e}")
            continue

    print(f"Collected {len(brand_names)} brands for further processing.")
    print("Brands:", brand_names)

    # Optional: Save the brand names to a text file for further analysis
    with open("data/make_names.txt", "w", encoding="utf-8") as f:
        for brand in brand_names:
            f.write(brand + "\n")

    # Prepare the CSV file for saving the Brand-Model pairs
    csv_file = open("data/brands_and_models.csv", mode="w", newline="", encoding="utf-8")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Brand", "Model"])  # Column headers

    # Define the base URL for accessing each brand's page
    base_url = "https://www.otomoto.pl/osobowe/{make_slug}?search%5Border%5D=relevance_web&search%5Badvanced_search_expanded%5D=true"

    # Iterate over each brand and process it
    for brand in brand_names:
        try:
            process_brand(driver, brand, base_url, csv_writer, custom_brand_slugs)
        except Exception as e:
            print(f"Unexpected error while processing brand {brand}: {e}")
            driver.save_screenshot(f"error_unexpected_brand_{brand}.png")
            continue  # Proceed to the next brand

    # Close the CSV file after writing all data
    csv_file.close()
    print("Data successfully saved to 'brands_and_models.csv'.")

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    # Close the browser once processing is complete
    driver.quit()
    print("Browser closed.")