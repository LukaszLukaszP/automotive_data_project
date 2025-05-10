def clean_battery_capacity(series, expected_unit='kWh'):
    # Extract the numeric value and its unit from each string
    extracted = series.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>\w+)', expand=True)

    # Identify any units that differ from the expected unit
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units found: {unexpected_units.tolist()}")

    # Convert the extracted values to floats and return
    return extracted['value'].astype(float)


def clean_range_column(series, expected_unit='km'):
    # Extract the numeric part and the unit from the range strings
    extracted = series.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>\w+)', expand=True)

    # Check for any units not matching the expected unit
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in range data: {unexpected_units.tolist()}")

    # Return the numeric values as floats
    return extracted['value'].astype(float)


def clean_engine_displacement(series, expected_unit='cm3'):
    # Ensure strings and trim whitespace
    series = series.astype(str).str.strip()

    # Extract numeric part and unit, allowing letters+digits in unit
    extracted = series.str.extract(
        r'(?P<value>[\d\s]+(?:[.,]\d+)?)[\s\u00A0]*?(?P<unit>[a-zA-Z0-9/]+)$',
        expand=True
    )

    # Clean value: remove spaces and unify decimal point
    extracted['value'] = (
        extracted['value']
        .str.replace(r'\s+', '', regex=True)
        .str.replace(',', '.')
    )

    # Validate units
    non_null_units = extracted['unit'].dropna()
    unexpected = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected) > 0:
        raise ValueError(f"Unexpected units in engine displacement: {unexpected.tolist()}")

    return extracted['value'].astype(float)


def clean_moc_column(series):
    # Remove spaces to normalize strings (e.g., '1 000 kW' -> '1000kW')
    cleaned = series.str.replace(' ', '', regex=False)

    # Extract numeric value and unit from the cleaned strings
    extracted = cleaned.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>\w+)', expand=True)

    # Normalize unit strings to lowercase
    extracted['unit'] = extracted['unit'].str.lower()

    # Define allowed power units
    expected_units = ['km', 'kw']

    # Flag any units outside the expected set
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.isin(expected_units)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in power data: {unexpected_units.tolist()}")

    # Convert extracted values to float
    extracted['value'] = extracted['value'].astype(float)

    # Convert kW values to horsepower (1 kW = 1.35962 HP), leave HP as-is
    extracted['value'] = extracted.apply(
        lambda row: row['value'] * 1.35962 if row['unit'] == 'kw' else row['value'],
        axis=1
    )

    return extracted['value']


def clean_co2_emissions_column(series, expected_unit='g/km'):
    # Remove spaces to consolidate unit formatting (e.g., '96 g/km' -> '96g/km')
    cleaned = series.str.replace(' ', '', regex=False)

    # Extract the numeric CO2 value and its unit
    extracted = cleaned.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>.+)', expand=True)

    # Ensure all extracted units match the expected unit
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in CO2 emissions: {unexpected_units.tolist()}")

    # Return emissions values as floats
    return extracted['value'].astype(float)


def clean_urban_fuel_column(series, expected_unit='l/100km'):
    # Remove spaces for consistent formatting (e.g., '7.8 l/100km' -> '7.8l/100km')
    cleaned = series.str.replace(' ', '', regex=False)

    # Extract numeric consumption value and unit
    extracted = cleaned.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>.+)', expand=True)

    # Validate that units match the expected format
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in urban fuel consumption: {unexpected_units.tolist()}")

    # Return consumption as float
    return extracted['value'].astype(float)


def clean_extraurban_fuel_column(series, expected_unit='l/100km'):
    # Strip spaces to unify formatting
    cleaned = series.str.replace(' ', '', regex=False)

    # Pull out the numeric value and its unit
    extracted = cleaned.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>.+)', expand=True)

    # Check units against the expected pattern
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in extra-urban fuel consumption: {unexpected_units.tolist()}")

    # Return the numeric consumption values as floats
    return extracted['value'].astype(float)


def clean_mileage_column(series, expected_unit='km'):
    # Remove thousand separators (spaces) for consistency
    cleaned = series.str.replace(' ', '', regex=False)

    # Extract the mileage number and its unit
    extracted = cleaned.str.extract(r'(?P<value>\d+)\s*(?P<unit>\w+)', expand=True)

    # Validate the mileage units
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in mileage data: {unexpected_units.tolist()}")

    # Return mileage values as integers
    return extracted['value'].astype(int)


def clean_avg_energy_consumption_column(series, expected_unit='kWh/100km'):
    # Remove spaces to normalize strings (e.g., '15 kWh/100km' -> '15kWh/100km')
    cleaned = series.str.replace(' ', '', regex=False)

    # Extract consumption values and units
    extracted = cleaned.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>.+)', expand=True)

    # Check that the units are as expected
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in average energy consumption: {unexpected_units.tolist()}")

    # Return numeric energy consumption as floats
    return extracted['value'].astype(float)


def clean_battery_health_column(series, expected_unit='%'):
    # Remove spaces for uniform formatting (e.g., '98 %' -> '98%')
    cleaned = series.str.replace(' ', '', regex=False)

    # Extract the health percentage and its unit symbol
    extracted = cleaned.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>[%])', expand=True)

    # Validate the unit symbol
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in battery health data: {unexpected_units.tolist()}")

    # Return battery health percentages as floats
    return extracted['value'].astype(float)


def clean_max_electric_power_column(series, expected_unit='HP'):
    # Remove spaces to standardize formatting (e.g., '120 HP' -> '120HP')
    cleaned = series.str.replace(' ', '', regex=False)

    # Extract the power value and its unit
    extracted = cleaned.str.extract(r'(?P<value>\d+\.?\d*)\s*(?P<unit>\w+)', expand=True)

    # Ensure the unit matches expected horsepower notation
    non_null_units = extracted['unit'].dropna()
    unexpected_units = non_null_units[~non_null_units.eq(expected_unit)].unique()
    if len(unexpected_units) > 0:
        raise ValueError(f"Unexpected units in max electric power: {unexpected_units.tolist()}")

    # Return the power values as floats
    return extracted['value'].astype(float)
