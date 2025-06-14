import csv
import os
from typing import List

from abstract.filemetadata import FileMetaData
from abstract.metadatawriter import MetaDataWriter


class CSVMetaDataWriter(MetaDataWriter):
    def write(self, data: List[FileMetaData], output_path: str):
        if not data:
            print("No data to write.")
            return
        try:
            with open(output_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].to_dict().keys())
                file_exists = os.path.exists(output_path)
                is_empty = not file_exists or os.stat(output_path).st_size == 0
                if is_empty:
                    writer.writeheader()
                for item in data:
                    writer.writerow(item.to_dict())
            print(f"Metadata written to : {output_path}")
        except PermissionError:
            print(f"Failed to write file. Please close the file and try again.")
