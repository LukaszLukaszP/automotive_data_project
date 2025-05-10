import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from itertools import chain
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
from scripts.utils.equipment_utils import (
    extract_equipment_list,
    build_equipment_df,
    generate_listing_equipment_relations
)
from scripts.utils.data_cleaning_utils import (
    clean_battery_capacity,
    clean_range_column,
    clean_engine_displacement,
    clean_moc_column,
    clean_co2_emissions_column,
    clean_urban_fuel_column,
    clean_extraurban_fuel_column,
    clean_mileage_column,
    clean_avg_energy_consumption_column,
    clean_battery_health_column,
    clean_max_electric_power_column
)

# Load the merged dataset from CSV
df = pd.read_csv(
    'C:/Users/Lukasz Pindus/VS Code Python/automotive_data_project/data/merged01.csv',
    dtype={
        'Pojemność baterii': str,
        'Autonomia': str,
        'Średnie zużycie': str,
        'Kondycja baterii': str,
        'Typ złącza ładowania': str,
        'advert_date': str
    }
)

# Clean and convert various specification columns
# Battery capacity to numeric kWh
df['Pojemność_baterii_kWh'] = clean_battery_capacity(df['Pojemność baterii'])
df = df.drop('Pojemność baterii', axis=1)

# Range (Autonomia) to numeric km
df['Autonomia_km'] = clean_range_column(df['Autonomia'])
df = df.drop('Autonomia', axis=1)

# Engine displacement (cm3)
df['engine_capacity_cm3'] = clean_engine_displacement(df['Pojemność skokowa'])
df = df.drop('Pojemność skokowa', axis=1)

# Power in horsepower
df['power_hp'] = clean_moc_column(df['Moc'])
df = df.drop('Moc', axis=1)

# CO2 emissions in g/km
df['co2_emissions_gpkm'] = clean_co2_emissions_column(df['Emisja CO2'])
df = df.drop('Emisja CO2', axis=1)

# Urban and extra-urban fuel consumption (L/100km)
df['urban_fuel_consumption_l_per_100km'] = clean_urban_fuel_column(df['Spalanie W Mieście'])
df = df.drop('Spalanie W Mieście', axis=1)
df['extraurban_fuel_consumption_l_per_100km'] = clean_extraurban_fuel_column(df['Spalanie Poza Miastem'])
df = df.drop('Spalanie Poza Miastem', axis=1)

# Mileage in kilometers
df['mileage_km'] = clean_mileage_column(df['Przebieg'])
df = df.drop('Przebieg', axis=1)

# Average energy consumption (kWh/100km)
df['average_energy_consumption_kwh_per_100km'] = clean_avg_energy_consumption_column(df['Średnie zużycie'])
df = df.drop('Średnie zużycie', axis=1)

# Battery health as percentage
df['battery_health_percent'] = clean_battery_health_column(df['Kondycja baterii'])
df = df.drop('Kondycja baterii', axis=1)

# Maximum electric power in HP
df['max_electric_power_hp'] = clean_max_electric_power_column(df['Elektryczna moc maksymalna HP'])
df = df.drop('Elektryczna moc maksymalna HP', axis=1)

# Clean price column: remove spaces, replace commas, convert to float
df['price'] = (
    df['price']
    .str.replace(' ', '', regex=False)
    .str.replace(',', '.', regex=False)
    .apply(pd.to_numeric, errors='coerce')
)

# Map Polish month names to numeric strings for advert_date parsing
month_map = {
    'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
    'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
    'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
}

# Replace Polish month names in advert_date string
for pl, num in month_map.items():
    df['advert_date'] = df['advert_date'].str.replace(pl, num, regex=False)

# Convert advert_date to datetime type
df['advert_date'] = pd.to_datetime(df['advert_date'], format='%d %m %Y %H:%M')

# Identify text, numeric, and date columns
text_cols = df.select_dtypes(include='object').columns
num_cols = df.select_dtypes(include=['float64', 'int64']).columns
date_cols = df.select_dtypes(include='datetime64[ns]').columns

# Temporarily fill missing values for de-duplication
# Text with empty string, numbers with -1, dates with a placeholder
df[text_cols] = df[text_cols].fillna('')
df[num_cols] = df[num_cols].fillna(-1)
df[date_cols] = df[date_cols].fillna(pd.Timestamp('1900-01-01'))

df = df.drop_duplicates()

# Restore proper missing indicators: empty → <NA>, -1 → <NA>, placeholder date → NaT
df[text_cols] = df[text_cols].replace('', pd.NA)
df[num_cols] = df[num_cols].replace(-1, pd.NA)
df[date_cols] = df[date_cols].replace(pd.Timestamp('1900-01-01'), pd.NaT)

# Rename columns from Polish to English identifiers
column_mapping = {
    'Marka pojazdu': 'make', 'Model pojazdu': 'model', 'Wersja': 'version',
    'Kolor': 'color', 'Liczba drzwi': 'number_of_doors', 'Liczba miejsc': 'number_of_seats',
    'Rok produkcji': 'production_year', 'Generacja': 'generation', 'Rodzaj paliwa': 'fuel_type',
    'Typ nadwozia': 'body_type', 'Rodzaj koloru': 'color_type', 'Skrzynia biegów': 'transmission',
    'Napęd': 'drive_type', 'Kraj pochodzenia': 'country_of_origin', 'Numer rejestracyjny pojazdu': 'registration_number',
    'Stan': 'condition', 'Bezwypadkowy': 'accident_free',
    'Data pierwszej rejestracji w historii pojazdu': 'first_registration_date',
    'Zarejestrowany w Polsce': 'registered_in_poland', 'Pierwszy właściciel (od nowości)': 'first_owner',
    'Serwisowany w ASO': 'serviced_at_authorized_station', 'Ma numer rejestracyjny': 'has_registration_number',
    'Typ złącza ładowania': 'charging_connector_type', 'Liczba silników': 'number_of_engines',
    'Odzyskiwanie energii hamowania': 'brake_energy_recovery', 'Liczba baterii': 'number_of_batteries',
    'equipment': 'equipment', 'price': 'price', 'currency': 'currency', 'price_level': 'price_level',
    'advert_date': 'advert_date', 'advert_id': 'advert_id', 'description': 'description',
    'Pojemność_baterii_kWh': 'battery_capacity_kwh', 'Autonomia_km': 'range_km',
    'engine_capacity_cm3': 'engine_capacity_cm3', 'power_hp': 'power_hp',
    'co2_emissions_gpkm': 'co2_emissions_gpkm',
    'urban_fuel_consumption_l_per_100km': 'urban_fuel_consumption_l_per_100km',
    'extraurban_fuel_consumption_l_per_100km': 'extraurban_fuel_consumption_l_per_100km',
    'mileage_km': 'mileage_km', 'average_energy_consumption_kwh_per_100km': 'average_energy_consumption_kwh_per_100km',
    'battery_health_percent': 'battery_health_percent', 'max_electric_power_hp': 'max_electric_power_hp'
}

df.rename(columns=column_mapping, inplace=True)

# Define desired dtypes for columns
dtype_conversion = {
    'number_of_doors': 'Int8', 'number_of_seats': 'Int8', 'production_year': 'Int16',
    'number_of_engines': 'Int8', 'number_of_batteries': 'Int8', 'mileage_km': 'Int32',
    'advert_id': 'Int64',
    'price': 'float32', 'battery_capacity_kwh': 'float32', 'range_km': 'float32',
    'power_hp': 'float32', 'co2_emissions_gpkm': 'float32', 'urban_fuel_consumption_l_per_100km': 'float32',
    'extraurban_fuel_consumption_l_per_100km': 'float32', 'average_energy_consumption_kwh_per_100km': 'float32',
    'battery_health_percent': 'float32', 'max_electric_power_hp': 'float32', 'engine_capacity_cm3': 'float32',
    'accident_free': 'boolean', 'registered_in_poland': 'boolean', 'first_owner': 'boolean',
    'serviced_at_authorized_station': 'boolean', 'has_registration_number': 'boolean', 'brake_energy_recovery': 'boolean',
    'make': 'category', 'model': 'category', 'version': 'category', 'color': 'category',
    'generation': 'category', 'fuel_type': 'category', 'body_type': 'category',
    'color_type': 'category', 'transmission': 'category', 'drive_type': 'category',
    'country_of_origin': 'category', 'condition': 'category', 'charging_connector_type': 'category',
    'currency': 'category', 'price_level': 'category'
}

date_columns = ['first_registration_date', 'advert_date']
nullable_float_types = ['float32']
nullable_int_types = ['Int8', 'Int16', 'Int32', 'Int64']

missing_columns = []

# Convert boolean-like string columns to actual boolean dtype
bool_cols = [
    'accident_free', 'registered_in_poland', 'first_owner',
    'serviced_at_authorized_station', 'has_registration_number',
    'brake_energy_recovery'
]
mapping = {'tak': True, 'nie': False}

for col in bool_cols:
    s = df[col].astype(str).str.strip().str.lower()
    df[col] = s.map(mapping).astype('boolean')

# Apply numeric and categorical dtype conversions
for col, target_type in dtype_conversion.items():
    if col not in df.columns:
        missing_columns.append(col)
        continue
    if target_type in nullable_float_types:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(target_type)
    elif target_type in nullable_int_types:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(target_type)
    elif target_type == 'boolean':
        # Already cleaned above
        df[col] = df[col].astype('boolean')
    else:
        df[col] = df[col].astype(target_type)

# Ensure date columns are proper datetime types
for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    else:
        missing_columns.append(col)

# Report any missing columns that couldn't be converted
if missing_columns:
    print("⚠️ Missing columns (not converted):")
    for col in missing_columns:
        print(f" - {col}")
else:
    print("✅ All columns converted successfully.")

# Add a local unique identifier for each listing
df = df.reset_index(drop=True).copy()
df['local_id'] = df.index + 1

# Extract equipment lists and build relational tables
df['equipment_list'] = extract_equipment_list(df['equipment'])
equipment_df = build_equipment_df(df['equipment_list'])
listing_equipment_df = generate_listing_equipment_relations(df, equipment_df)

# Save cleaned listings and equipment CSVs to disk
output_dir = r'C:\Users\Lukasz Pindus\VS Code Python\automotive_data_project\data'
listings_df = df.drop(columns=['equipment', 'equipment_list'])
listings_df.to_csv(os.path.join(output_dir, 'listings.csv'), index=False)
equipment_df.to_csv(os.path.join(output_dir, 'equipment_options.csv'), index=False)
listing_equipment_df.to_csv(os.path.join(output_dir, 'listing_equipment.csv'), index=False)