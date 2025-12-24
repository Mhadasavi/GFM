import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(job_name: str, log_dir: str):
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    log_file = f"{job_name}_{datetime.now():%Y-%m-%d}.log"
    log_path = Path(log_dir) / log_file

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    if root.handlers:
        return

    # File (DEBUG+)
    file_handler = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=5)
    file_handler.setLevel(logging.DEBUG)

    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-8s %(name)s - %(message)s")
    )

    # Console (INFO+)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(levelname)-8s %(name)s - %(message)s")
    )

    root.addHandler(file_handler)
    root.addHandler(console_handler)
