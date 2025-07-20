import os.path
import time

from pandas import DataFrame
from urllib3.filepost import writer

from abstract.metadatawriter import MetaDataWriter
from feeds.csvmetadatawriter import CSVMetaDataWriter
from feeds.mongodbWriter import MongoDbWriter
from fetchers.drivemetadatascanner import DriveMetaDataScanner
from tools.google.googleauthenticator import GoogleAuthenticator
from tools.mastercsvmetadatawriter import MasterCsvMetaDataWriter


class DriveMetaDataExporter:
    def __init__(
        self,
        scanner: DriveMetaDataScanner,
        meta_writer: MetaDataWriter,
        db_writer: MongoDbWriter,
    ):
        self.scanner = scanner
        self.meta_writer = meta_writer
        self.db_writer = db_writer

    def meta_data_writer(self):
        return self.scanner.scan()

    def export(self, drive_metadata_df: DataFrame, output_path: str):
        self.meta_writer.write_df_to_csv(drive_metadata_df, output_path)

    def export_to_db(self, input_path: str, collection_name: str):
        self.db_writer.write(input_path, collection_name)


if __name__ == "__main__":
    credentials_path = "F:\\GFM Data\\client_secret.json"
    drive_scopes = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
    drive_creds = GoogleAuthenticator(
        credentials_path, drive_scopes, port=8080
    ).authenticate()

    start_time = time.time()

    drive_scanner = DriveMetaDataScanner(drive_creds)
    writer = MasterCsvMetaDataWriter()
    db_writer = MongoDbWriter()

    exporter = DriveMetaDataExporter(drive_scanner, writer, db_writer)
    file_name = f"drive_metadata.csv"
    output_csv = os.path.join("F:\\GFM Data\\drivemetadata\\", file_name)
    drive_metadata_df = exporter.meta_data_writer()

    exporter.export(drive_metadata_df, output_csv)
    exporter.export_to_db(output_csv, "driveMetaDataCollection")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Completed in {elapsed_time:.4f} seconds")
