from sqlalchemy import create_engine, text

# Connect to the PostgreSQL database
engine = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/car_database")

with engine.begin() as conn:
    # Drop the view and tables if they already exist
    conn.execute(text("DROP VIEW IF EXISTS listing_with_equipment_view"))
    conn.execute(text("DROP TABLE IF EXISTS listing_equipment"))
    conn.execute(text("DROP TABLE IF EXISTS equipment_options"))
    conn.execute(text("DROP TABLE IF EXISTS listings"))

    # Create the main listings table with an auto-incrementing primary key
    conn.execute(text("""
        CREATE TABLE listings (
            id SERIAL PRIMARY KEY,
            local_id INTEGER,
            make TEXT,
            model TEXT,
            version TEXT,
            color TEXT,
            number_of_doors SMALLINT,
            number_of_seats SMALLINT,
            production_year SMALLINT,
            generation TEXT,
            fuel_type TEXT,
            body_type TEXT,
            color_type TEXT,
            transmission TEXT,
            drive_type TEXT,
            country_of_origin TEXT,
            registration_number TEXT,
            condition TEXT,
            accident_free BOOLEAN,
            first_registration_date DATE,
            registered_in_poland BOOLEAN,
            first_owner BOOLEAN,
            serviced_at_authorized_station BOOLEAN,
            has_registration_number BOOLEAN,
            charging_connector_type TEXT,
            number_of_engines SMALLINT,
            brake_energy_recovery BOOLEAN,
            number_of_batteries SMALLINT,
            price NUMERIC,
            currency TEXT,
            price_level TEXT,
            advert_date TIMESTAMP,
            advert_id BIGINT,
            description TEXT,
            battery_capacity_kwh NUMERIC,
            range_km NUMERIC,
            engine_capacity_cm3 NUMERIC,
            power_hp NUMERIC,
            co2_emissions_gpkm NUMERIC,
            urban_fuel_consumption_l_per_100km NUMERIC,
            extraurban_fuel_consumption_l_per_100km NUMERIC,
            mileage_km INTEGER,
            average_energy_consumption_kwh_per_100km NUMERIC,
            battery_health_percent NUMERIC,
            max_electric_power_hp NUMERIC
        );
    """))

    # Create table for equipment options
    conn.execute(text("""
        CREATE TABLE equipment_options (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
    """))

    # Create relation table between listings and equipment
    conn.execute(text("""
        CREATE TABLE listing_equipment (
            listing_id INTEGER,
            equipment_id INTEGER,
            PRIMARY KEY (listing_id, equipment_id)
        );
    """))

    # Create a view to see listings with their equipment as a single string
    conn.execute(text("""
        CREATE VIEW listing_with_equipment_view AS
        SELECT
            l.*,
            string_agg(e.name, ', ') AS equipment
        FROM listings l
        LEFT JOIN listing_equipment le ON l.id = le.listing_id
        LEFT JOIN equipment_options e ON le.equipment_id = e.id
        GROUP BY l.id;
    """))

print("✅ Database schema and view created successfully.")