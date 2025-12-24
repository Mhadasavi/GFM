import logging
import os.path
import time

from abstract.filemetadatautils import FileMetaDataUtils
from abstract.metadatawriter import MetaDataWriter
from Config.globalconfig import GlobalConfig
from feeds.csvmetadatawriter import CSVMetaDataWriter
from feeds.filescanner import FileScanner
from feeds.mongodbWriter import MongoDbWriter


class FileMetaDataExporter:
    def __init__(
        self, scanner: FileScanner, writer: MetaDataWriter, db_writer: MongoDbWriter
    ):
        self.scanner = scanner
        self.writer = writer
        self.db_writer = db_writer

    def scan(self):
        return self.scanner.scan()

    def export_to_csv(self, raw_metadata, output_path: str):
        self.writer.write(raw_metadata, output_path)

    def export_to_db(self, input_path: str, collection_name: str):
        self.db_writer.write(input_path, collection_name)


logger = logging.getLogger("stage.local_raw_fetch")


def run():
    start_time = time.time()

    logger.info("Stage : local_raw_fetch started")

    config = GlobalConfig()

    try:
        folder_dir = input("Enter the directory: ")
        folder = os.path.expanduser(folder_dir)

        year = FileMetaDataUtils.get_year_from_path(folder)
        if year:
            logger.info(f"Year extracted from '{folder}': {year}")
        else:
            logger.warning(f"No valid 4-digit year found in '{folder}', using 0000")
            year = 0

        scanner = FileScanner(folder)
        writer = CSVMetaDataWriter()
        db_writer = MongoDbWriter()

        exporter = FileMetaDataExporter(scanner, writer, db_writer)

        file_type, raw_metadata = exporter.scan()

        file_name = f"file_metadata_{file_type}_{year}.csv"
        output_csv = os.path.join(config.raw_metadata_dir, file_name)

        exporter.export_to_csv(raw_metadata, output_csv)
        exporter.export_to_db(output_csv, "metaDataCollection")

        elapsed_time = time.time() - start_time
        logger.info(f"Completed in {elapsed_time:.2f} seconds")

    except Exception:
        logger.exception("local_raw_fetch failed")
        raise


if __name__ == "__main__":
    from app_logging.logging_config import setup_logging

    config = GlobalConfig()
    setup_logging("local_raw_fetch", config.log_path)

    run()
