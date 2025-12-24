import csv
import logging
import os
from abc import ABC
from typing import List

from idlelib.iomenu import encoding
from pandas import DataFrame

from abstract.filemetadata import FileMetaData
from abstract.metadatawriter import MetaDataWriter


class MasterCsvMetaDataWriter(MetaDataWriter):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def write(self, data: List[FileMetaData] | dict, output_path: str):
        if not data:
            self.logger.info("No data to write.")
            return
        try:
            # Get headers from the first nested dict
            headers = list(next(iter(data.values())).keys())
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                file_exists = os.path.exists(output_path)
                is_empty = not file_exists or os.stat(output_path).st_size == 0
                if is_empty:
                    writer.writeheader()
                for item in data.values():
                    writer.writerow(item)
            self.logger.info(f"Metadata written to : {output_path}")
        except PermissionError:
            self.logger.info(
                f"Failed to write file. Please close the file and try again."
            )

    def write_df_to_csv(self, metadata_df: DataFrame, output_path: str):
        if metadata_df.empty:
            self.logger.info("No data to write in metadata_df")
            return
        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            metadata_df.to_csv(output_path, index=False)
            self.logger.info(f"metadata_df written to : {output_path}")
        except PermissionError:
            self.logger.info(
                f"Failed to write file. Please close the file and try again."
            )
