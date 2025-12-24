import argparse
import logging
import sys

from app_logging.logging_config import setup_logging
from Config.globalconfig import GlobalConfig
from feeds import filemetadataexporter
from fetchers import drivemetadataexporter
from tools import feedmasterexporter

logger = logging.getLogger("pipeline")


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--stage",
            choices=[
                "local_raw_fetch",
                "local_raw_master",
                "gdrive_fetch",
                "compare",
                "all",
            ],
            required=True,
        )

        args = parser.parse_args()
        config = GlobalConfig()
        setup_logging(args.stage, config.log_path)

        if args.stage in ("local_raw_fetch", "all"):
            filemetadataexporter.run()

        if args.stage in ("local_raw_master", "all"):
            feedmasterexporter.run()

        if args.stage in ("gdrive_fetch", "all"):
            drivemetadataexporter.run()

        # if args.stage in ("compare", "all"):
        #     compare.run()
    except Exception:
        logger.error("Pipeline Execution failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
