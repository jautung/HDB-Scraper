# pylint: disable=import-error, missing-function-docstring, missing-module-docstring, logging-fstring-interpolation
import argparse
import logging
import file_util

logger = logging.getLogger("HDB Scraper")


def main():
    parser = argparse.ArgumentParser(description="MRT Precompute")
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()
    logger.setLevel(args.log_level)

    file_util.maybe_create_output_folder()


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(handler)

    main()
