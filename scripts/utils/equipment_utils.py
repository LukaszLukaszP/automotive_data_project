from itertools import chain
import pandas as pd

# Equipment sections to ignore when parsing individual items
EXCLUDED_EQUIPMENT_SECTIONS = {
    'audio i multimedia',
    'komfort i dodatki',
    'systemy wspomagania kierowcy',
    'bezpieczeństwo'
}

def extract_equipment_list(equipment_col):
    """
    Parse each raw equipment string into a cleaned list of unique, lowercase items.
    Excludes any general section headers defined in EXCLUDED_EQUIPMENT_SECTIONS.
    """
    # Replace NaN with empty string, split on '|', strip and lowercase items,
    # filter out sections to exclude, then dedupe using a set
    return equipment_col.fillna('').apply(
        lambda x: list(set(
            item.strip().lower()
            for item in x.split('|')
            if item.strip().lower() not in EXCLUDED_EQUIPMENT_SECTIONS
        ))
    )

def build_equipment_df(equipment_lists):
    """
    Construct a DataFrame of every unique equipment option with an incremental integer ID.
    """
    # Flatten all lists into a single set of unique names
    unique_equipment = set(chain.from_iterable(equipment_lists))
    # Create DataFrame sorted by name, then assign the index as the ID
    df = pd.DataFrame(sorted(unique_equipment), columns=['name'])
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'id'}, inplace=True)
    # Return sorted by name with clean indexing
    return df[['id', 'name']].sort_values(by='name').reset_index(drop=True)

def generate_listing_equipment_relations(df, equipment_df, equipment_column='equipment_list'):
    """
    Build a mapping DataFrame linking each listing's local_id to its equipment IDs.
    """
    # Create a lookup from equipment name to its ID
    equipment_map = dict(zip(equipment_df['name'], equipment_df['id']))
    records = []
    # For each listing, generate one row per equipment item
    for _, row in df.iterrows():
        listing_id = row['local_id']
        for eq_name in row[equipment_column]:
            eq_id = equipment_map.get(eq_name)
            if eq_id is not None:
                records.append({'listing_id': listing_id, 'equipment_id': eq_id})
    # Return as DataFrame
    return pd.DataFrame(records)