"""Retired legacy experiment.

This script used browser automation to reveal VIN/contact-adjacent page elements and
contained anti-detection settings. It is intentionally disabled. The supported MVP
does not collect VINs and does not bypass blocking or bot-detection signals.
"""


def main() -> None:
    raise SystemExit(
        "This legacy VIN/browser experiment is retired. "
        "Use `python -m automotive_data_project run-pipeline` instead."
    )


if __name__ == "__main__":
    main()
