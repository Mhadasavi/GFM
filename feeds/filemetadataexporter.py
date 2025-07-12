import logging
import os.path
import time

from abstract.filemetadatautils import FileMetaDataUtils
from abstract.metadatawriter import MetaDataWriter
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

    def meta_data_writer(self):
        return self.scanner.scan()

    def export(self, raw_metadata, output_path: str):
        self.writer.write(raw_metadata, output_path)

    def export_to_db(self, input_path: str, collection_name: str):
        self.db_writer.write(input_path, collection_name)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    start_time = time.time()
    folder_dir = input("enter the directory :")
    folder = os.path.expanduser(folder_dir)

    year = FileMetaDataUtils.get_year_from_path(folder)
    if year:
        logger.info(f"Year extracted from '{folder}': {year}")
    else:
        logger.warning(f"No valid 4-digit year found in '{folder}'")
        logger.info("Override year to 0000")
        year = 0000

    scanner = FileScanner(folder)
    writer = CSVMetaDataWriter()
    dbWriter = MongoDbWriter()

    exporter = FileMetaDataExporter(scanner, writer, dbWriter)

    file_type, raw_metadata = exporter.meta_data_writer()
    file_name = f"file_metadata_{file_type}_{year}.csv"
    output_csv = os.path.join("F:\\GFM Data\\metadata\\", file_name)

    exporter.export(raw_metadata, output_csv)

    exporter.export_to_db(output_csv, "metaDataCollection")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Completed in {elapsed_time:.4f} seconds")
