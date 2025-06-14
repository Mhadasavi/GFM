import logging
import os.path

from abstract.metadatawriter import MetaDataWriter
from tools.mastercsvmetadatawriter import MasterCsvMetaDataWriter
from tools.masterfilescanner import MasterFileScanner


class FeedMasterExporter:
    def __init__(
        self, master_scanner: MasterFileScanner, master_writer: MetaDataWriter
    ):
        self.master_scanner = master_scanner
        self.master_writer = master_writer

    # read all the csvs from the path
    def master_data_reader(self) -> dict:
        return self.master_scanner.scan()

    # Store the data in a set like structure to contains only unique entries
    # Create and save a new csv
    def master_export(self, data: dict, output_path: str):
        self.master_writer.write(data, output_path)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    folder_dir = input("Enter the Raw Csvs directory...")
    folder = os.path.expanduser(folder_dir)

    master_scanner = MasterFileScanner(folder)
    writer = MasterCsvMetaDataWriter()

    exporter = FeedMasterExporter(master_scanner, writer)
    file_name = f"file_metadata_master.csv"  # file_metadata_master.csv
    output_csv = os.path.join("F:\\GFM Data\\master\\", file_name)

    exporter.master_export(exporter.master_data_reader(), output_csv)
