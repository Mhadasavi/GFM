import csv
from typing import List

from feeds.filemetadata import FileMetaData
from feeds.metadatawriter import MetaDataWriter


class CSVMetaDataWriter(MetaDataWriter):
    def write(self, data:List[FileMetaData], output_path: str):
        if not data:
            print("No data to write.")
            return

        with open(output_path, "w", newline = "", encoding = "utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].to_dict().keys())
            writer.writeheader()
            for item in data:
                writer.writerow(item.to_dict())
        print(f'Metadata written to : {output_path}')