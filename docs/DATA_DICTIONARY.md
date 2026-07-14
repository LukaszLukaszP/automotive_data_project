# Data Dictionary

## `listings`

| Column | Type | Description |
| --- | --- | --- |
| `id` | integer | Internal primary key. |
| `advert_id` | string | Source advert identifier. Unique together with `source`. |
| `source` | string | Source system, currently `otomoto`. |
| `source_url` | text | Detail page URL used for extraction. |
| `make` | string | Vehicle make. |
| `model` | string | Vehicle model. |
| `version` | string | Version or trim text when available. |
| `production_year` | integer | Vehicle production year. |
| `price` | numeric | Parsed advert price. |
| `currency` | string | Currency text, usually `PLN`. |
| `mileage_km` | integer | Parsed mileage in kilometers. |
| `fuel_type` | string | Fuel type as displayed by source. |
| `transmission` | string | Transmission as displayed by source. |
| `body_type` | string | Body type as displayed by source. |
| `power_hp` | integer | Power normalized to horsepower. kW values are converted. |
| `engine_capacity_cm3` | integer | Engine capacity in cubic centimeters. |
| `advert_date` | datetime | Source advert date if detected. |
| `first_seen_at` | datetime | First time this advert was inserted into the database. |
| `last_seen_at` | datetime | Last time this advert was seen by the pipeline. |
| `scraped_at` | datetime | Timestamp of detail page parsing. |
| `equipment` | JSON | Equipment names as a simple list. |
| `raw_parameters` | JSON | Raw source label/value parameters for uncertain parsing. |

## Uniqueness

`source` and `advert_id` form the natural uniqueness rule. Repeated adverts are updated with UPSERT rather than inserted again.
