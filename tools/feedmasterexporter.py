import logging
import os.path
import time

from abstract.filemetadatautils import FileMetaDataUtils
from abstract.metadatawriter import MetaDataWriter
from Config.globalconfig import GlobalConfig
from feeds.mongodbWriter import MongoDbWriter
from tools.mastercsvmetadatawriter import MasterCsvMetaDataWriter
from tools.masterfilescanner import MasterFileScanner


class FeedMasterExporter:
    def __init__(
        self,
        master_scanner: MasterFileScanner,
        master_writer: MetaDataWriter,
        db_writer: MongoDbWriter,
    ):
        self.master_scanner = master_scanner
        self.master_writer = master_writer
        self.db_writer = db_writer

    # read all the csvs from the path
    def master_data_reader(self) -> dict:
        return self.master_scanner.scan()

    # Store the data in a set like structure to contains only unique entries
    # Create and save a new csv
    def master_export(self, data: dict, output_path: str):
        self.master_writer.write(data, output_path)

    def master_export_to_db(self, input_path: str, collection_name: str):
        self.db_writer.write(input_path, collection_name)


logger = logging.getLogger("stage.local_raw_master")


def run():
    start_time = time.time()

    logger.info("Stage : local_raw_master started")

    config = GlobalConfig()
    try:
        folder_dir = input("Enter the Raw Csvs directory...")
        folder = os.path.expanduser(folder_dir)
        master_scanner = MasterFileScanner(folder)
        writer = MasterCsvMetaDataWriter()
        dbWriter = MongoDbWriter()

        exporter = FeedMasterExporter(master_scanner, writer, dbWriter)
        file_name = f"file_metadata_master.csv"
        output_csv = os.path.join(config.master_metadata_dir, file_name)

        exporter.master_export(exporter.master_data_reader(), output_csv)
        exporter.master_export_to_db(output_csv, "MasterDataCollection")

        elapsed_time = time.time() - start_time
        logger.info(f"Completed in {elapsed_time:.2f} seconds")

    except Exception:
        logger.exception("local_raw_master failed")
        raise


if __name__ == "__main__":
    from app_logging.logging_config import setup_logging

    config = GlobalConfig()
    setup_logging("local_raw_master", config.log_path)

    run()
