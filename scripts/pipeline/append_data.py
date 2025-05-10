import pandas as pd
from sqlalchemy import create_engine, text

# Connect to the PostgreSQL database
engine = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/car_database")

# Define your local CSV path
data_dir = "C:/Users/Lukasz Pindus/VS Code Python/automotive_data_project/data"

# Load the cleaned CSVs into pandas
df_listings = pd.read_csv(f"{data_dir}/listings.csv")
df_equipment = pd.read_csv(f"{data_dir}/equipment_options.csv")
df_link = pd.read_csv(f"{data_dir}/listing_equipment.csv")

# Clean null or empty names before inserting
df_equipment = df_equipment[df_equipment['name'].notna() & (df_equipment['name'].str.strip() != '')]

# Append equipment options and listings
df_equipment.to_sql("equipment_options", engine, if_exists="append", index=False)
df_listings.to_sql("listings", engine, if_exists="append", index=False)

# Store temporary link table (using local_id for now)
df_link.to_sql("temp_listing_equipment", engine, if_exists="replace", index=False)

# Remap local_id to real PostgreSQL auto-generated id and insert
with engine.begin() as conn:
    conn.execute(text("""
        INSERT INTO listing_equipment (listing_id, equipment_id)
        SELECT l.id, tle.equipment_id
        FROM temp_listing_equipment tle
        JOIN listings l ON l.local_id = tle.listing_id
        ON CONFLICT DO NOTHING
    """))

    # Clean up temporary table
    conn.execute(text("DROP TABLE temp_listing_equipment"))

print("✅ New data successfully appended and ID remapping completed.")