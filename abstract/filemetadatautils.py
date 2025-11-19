import csv
import gzip
import hashlib
import io
import logging
import os
import re
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

from fetchers import Config


class FileMetaDataUtils:

    file_type_dict = {
        "jpeg": "img",
        "jpg": "img",
        "png": "img",
        "gif": "img",
        "bmp": "img",
    }

    def extract_size_and_unit_series(size_series: pd.Series):
        # convert bytes → string sizes
        size_str = size_series.apply(FileMetaDataUtils.convert_size)

        # extract numeric part
        numeric = size_str.str.extract(r"([0-9.]+)")[0].astype(float)

        # extract unit
        unit = size_str.str.extract(r"([A-Za-z]+)")[0]

        return numeric, unit

    @staticmethod
    def extract_size_and_unit(size):
        if pd.isnull(size):
            return 0.00, ""
        size_str = FileMetaDataUtils.convert_size(int(size))
        return float(size_str[:-2]), size_str[-2:]

    @staticmethod
    def convert_size(size_bytes):
        try:
            size_bytes = int(size_bytes)
        except (TypeError, ValueError):
            return "0Bytes"

        for unit in ["Bytes", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.2f}{unit}"
            size_bytes /= 1024

    @staticmethod
    def get_file_type(file_name: str) -> str | None:
        if not file_name:
            return None

        matcher = re.search("[^.]+$", file_name)

        if not matcher:
            return None

        extension = matcher.group(0).lower()
        return FileMetaDataUtils.file_type_dict.get(extension, extension)

    """Extracts the first 4-digit year from a given folder path string."""

    @staticmethod
    def get_year_from_path(folder: str) -> str | None:
        matcher = re.search(r"\d{4}", folder)
        if matcher:
            year = matcher.group(0)
            return year
        else:
            return None

    @staticmethod
    def get_file_hash(fields: list[str]) -> str:
        hash_input = "|".join(fields)
        return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

    @staticmethod
    def ensure_dir(path: str) -> None:
        if path and not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def file_size_mb(path: str) -> float:
        if not os.path.exists(path):
            return 0.0
        return os.path.getsize(path) / (1024 * 1024)

    @staticmethod
    def today_str() -> str:
        # yyyy-mm-dd (local)
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def make_base_stem(cfg: Config) -> str:
        date_part = f"_{FileMetaDataUtils.today_str()}" if cfg.daily_naming else ""
        return f"{cfg.base_filename}{date_part}"

    def with_batch_name(cfg: Config, batch_index: int) -> str:
        stem = FileMetaDataUtils.make_base_stem(cfg)
        return f"{stem}_batch{batch_index}.csv"

    def make_paths(cfg: Config, batch_index: int) -> Tuple[str, str]:
        """
        Returns (final_path, temp_path).
        Applies compression if enabled.
        """
        FileMetaDataUtils.ensure_dir(cfg.output_dir)
        filename = FileMetaDataUtils.with_batch_name(cfg, batch_index)
        if cfg.compression == "gzip":
            filename += ".gz"
        final_path = os.path.join(cfg.output_dir, filename)
        tmp_path = final_path + cfg.temp_suffix
        return final_path, tmp_path

    def read_last_line_text(path: str) -> Optional[str]:
        """Read last line of a text file (works for .csv or .csv.gz)."""
        if not os.path.exists(path):
            return None

        if path.endswith(".gz"):
            with gzip.open(path, "rt", newline="") as f:
                last = None
                for line in f:
                    last = line
                return last
        else:
            with open(path, "rb") as f:
                if f.seek(0, io.SEEK_END) == 0:
                    return None
                # read backwards to first newline
                pos = f.tell() - 1
                while pos > 0:
                    f.seek(pos)
                    if f.read(1) == b"\n":
                        break
                    pos -= 1
                return f.readline().decode("utf-8", errors="ignore")

    def extract_first_column_from_csv_line(line: str) -> Optional[str]:
        """Assumes CSV, returns the first column (ID). Handles simple comma escaping via csv.reader."""
        if not line:
            return None
        reader = csv.reader([line])
        row = next(reader, [])
        if not row:
            return None
        return row[0].strip() if row[0] else None
