"""Retired CSV loader.

The supported loader is `python -m automotive_data_project run-pipeline`, which
uses SQLAlchemy transactions and UPSERT by `source` + `advert_id`.
"""


def main() -> None:
    raise SystemExit("This legacy CSV loader is retired. Use `python -m automotive_data_project run-pipeline`.")


if __name__ == "__main__":
    main()
