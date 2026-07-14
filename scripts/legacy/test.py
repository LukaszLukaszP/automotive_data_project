"""Retired legacy browser parsing experiment.

The supported parser is tested with local HTML fixtures under `tests/fixtures`.
Browser automation and VIN reveal flows are intentionally not part of the MVP.
"""


def main() -> None:
    raise SystemExit(
        "This legacy browser experiment is retired. "
        "Use `python -m automotive_data_project parse-fixture tests/fixtures/offer_complete.html` instead."
    )


if __name__ == "__main__":
    main()
