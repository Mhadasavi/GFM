import csv
import logging
import os


class MasterFileScanner:
    def __init__(self, folder_path: str):
        self.logger = logging.getLogger(__name__)
        self.folder_path = folder_path

    def scan(self) -> dict:
        result_dict = {}
        number_of_duplicates = 0
        # p = Path(self.folder_path)
        for filename in os.listdir(self.folder_path):
            file_path = os.path.join(self.folder_path, filename)
            if os.path.isfile(file_path):
                with open(file_path, mode="r", encoding="utf-8") as f:
                    self.logger.info(f"Reading {filename}")
                    csv_file = csv.DictReader(f)
                    for row in csv_file:
                        file_name = row["file_name"]
                        if file_name in result_dict:
                            self.logger.warning(
                                f"Duplicate file name found : {file_name}, skipping ..."
                            )
                            number_of_duplicates += 1
                        result_dict[file_name] = row
        if number_of_duplicates:
            self.logger.info(
                f"Found {number_of_duplicates} Duplicates in {self.folder_path}."
            )

        return result_dict
