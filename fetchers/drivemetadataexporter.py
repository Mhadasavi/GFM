import logging
import os.path
import time
from typing import Dict, List, Optional, Tuple

from google.oauth2.credentials import Credentials
from pandas import DataFrame
from urllib3.filepost import writer

from abstract.filemetadatautils import FileMetaDataUtils
from abstract.metadatawriter import MetaDataWriter
from feeds.csvmetadatawriter import CSVMetaDataWriter
from feeds.mongodbWriter import MongoDbWriter
from fetchers.checkpointmanager import CheckpointManager
from fetchers.Config import DownloaderConfig
from fetchers.csvchunkwriter import CSVChunkWriter
from fetchers.drivefetcher import DriveFetcher
from fetchers.drivemetadatascanner import DriveMetaDataScanner
from fetchers.ratelimiter import RateLimiter
from tools.google.googleauthenticator import GoogleAuthenticator
from tools.mastercsvmetadatawriter import MasterCsvMetaDataWriter


class DriveMetaDataExporter:
    """
    - Chunked API reading
    - Stream writing to CSV
    - Resume from last ID (and checkpoint)
    - Checkpoint files
    - Write after every page
    - File rotation / splitting
    - Daily/Batch naming
    - Temporary file + rename (chunk temp + atomic replace)
    - Permission handling (retries)
    - API quota respect (rate limiter)
    - Logging
    - Append to existing metadata (skip via last_id)
    """

    def __init__(
        self,
        scanner: DriveMetaDataScanner,
        meta_writer: MetaDataWriter,
        db_writer: MongoDbWriter,
        credentials: Credentials,
        cfg: DownloaderConfig,
        logger: logging.Logger,
    ):
        self.scanner = scanner
        self.meta_writer = meta_writer
        self.db_writer = db_writer
        self.cfg = cfg
        self.logger = logging.getLogger(__name__)
        self.cp = CheckpointManager(cfg, logger)
        self.writer = CSVChunkWriter(cfg, logger)
        self.rate = RateLimiter(cfg.quota_sleep_sec)
        self.credentials = credentials
        self.fetcher = DriveFetcher(credentials, cfg, logger, self.rate)

    # def meta_data_writer(self):
    #     return self.scanner.scan()

    # def export(self, drive_metadata_df: DataFrame, output_path: str):
    #     self.meta_writer.write_df_to_csv(drive_metadata_df, output_path)

    # def export_to_db(self, input_path: str, collection_name: str):
    #     self.db_writer.write(input_path, collection_name)

    def _discover_resume_points(self) -> Tuple[Optional[str], Optional[str], int]:
        cp = self.cp.load()
        last_id = cp.get("last_id")
        page_token = cp.get("page_token")
        batch_index = cp.get("batch_index", self.writer.get_batch_index())

        # If appending to an existing file without checkpoint, try last line for last_id
        if self.cfg.resume and not last_id and self.cfg.append_skip_existing:
            # read the *current* final file's last line
            current_final = self.writer.get_current_output_path()
            last_line = FileMetaDataUtils.read_last_line_text(current_final)
            if last_line:
                parsed = FileMetaDataUtils.extract_first_column_from_csv_line(last_line)
                # avoid treating header as ID
                if parsed and parsed != "id":
                    last_id = parsed
                    self.logger.info(f"Derived last_id from existing CSV: {last_id}")

        return last_id, page_token, batch_index

    def _skip_until_last_id(
        self, files: List[Dict], last_id: str
    ) -> Tuple[List[Dict], bool]:
        """
        Skip items until last_id encountered. Returns (remaining_files, found).
        """
        for i, f in enumerate(files):
            if f.get("id") == last_id:
                return files[i + 1 :], True
        return files, False

    def _save_checkpoint_safely(
        self, last_id: Optional[str], page_token: Optional[str]
    ):
        # Include the current batch index so rotation persists across restarts
        self.cp.save(
            last_id=last_id,
            page_token=page_token,
            batch_index=self.writer.get_batch_index(),
        )

    def run(self):
        last_id, page_token, batch_index = self._discover_resume_points()

        # Ensure writer batch matches checkpoint (e.g., after rotation).
        if batch_index != self.writer.get_batch_index():
            # Fast-forward writer's batch index by recreating with correct starting index.
            self.writer.batch_index = batch_index
            self.writer.final_path, self.writer.tmp_path = FileMetaDataUtils.make_paths(
                self.cfg, batch_index
            )
            self.writer.header_written_in_this_file = os.path.exists(
                self.writer.final_path
            )

        page_counter = 0
        for resp in self.fetcher.iterate_pages(start_token=page_token):
            page_counter += 1
            files = resp.get("files", []) or []

            # If resuming, skip until we pass last_id once
            if last_id and self.cfg.append_skip_existing:
                files, found = self._skip_until_last_id(files, last_id)
                if found:
                    # we consumed the page that had last_id; subsequent pages are full
                    last_id = None
                else:
                    # still haven't found last_id; continue to next page
                    self._save_checkpoint_safely(
                        last_id=last_id, page_token=resp.get("nextPageToken")
                    )
                    if page_counter % self.cfg.log_every_n_pages == 0:
                        self.logger.info(
                            f"Skipping page {page_counter} (still searching last_id)."
                        )
                    continue

            # Write-after-every-page (stream)
            if files:
                self.writer.write_chunk(files)
                last_written_id = files[-1].get("id")
            else:
                last_written_id = last_id  # no progress on this page

            # Save checkpoint *after* each write to minimize data loss
            self._save_checkpoint_safely(
                last_id=last_written_id, page_token=resp.get("nextPageToken")
            )

            if page_counter % self.cfg.log_every_n_pages == 0:
                self.logger.info(
                    f"Wrote page {page_counter} | rows={len(files)} | "
                    f"current_file={self.writer.get_current_output_path()} | "
                    f"size={FileMetaDataUtils.file_size_mb(self.writer.get_current_output_path()):.2f}MB"
                )

        self.logger.info("Download complete.")

        # -----------------------------------------------------
        # ✅ Verification Logic
        # -----------------------------------------------------

    def verify(self):
        self.logger.info("Starting verification process...")

        api_ids = self.fetcher.fetch_all_ids()
        csv_ids = self.fetcher.read_csv_ids()

        missing, extra = self._compare_ids(api_ids, csv_ids)

        self.logger.info(f"Total Drive items found via API: {len(api_ids)}")
        self.logger.info(f"Total rows in CSV: {len(csv_ids)}")

        if missing:
            self.logger.warning(f"⚠️ Missing {len(missing)} files not present in CSV.")
            self._write_diff_to_file(self.cfg.missing_ids, missing)

        if extra:
            self.logger.warning(f"⚠️ {len(extra)} extra rows in CSV not found in Drive.")
            self._write_diff_to_file(self.cfg.extra_ids, extra)

        if not missing and not extra:
            self.logger.info("✅ Verification successful — all files accounted for.")

    # @staticmethod
    def _compare_ids(self, api_ids, csv_ids):
        missing = api_ids - csv_ids
        extra = csv_ids - api_ids
        return missing, extra

    def _write_diff_to_file(self, filename, diff_set):
        path = os.path.join(self.cfg.output_dir, filename)
        with open(path, "w") as f:
            for i in diff_set:
                f.write(f"{i}\n")
        self.logger.info(f"Wrote diff file: {path}")


def bootstrap_logger(config: DownloaderConfig):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
        filename=config.log_path,
        filemode="w",
    )

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)


if __name__ == "__main__":

    start_time = time.time()
    config = DownloaderConfig()
    bootstrap_logger(config)
    drive_creds = GoogleAuthenticator(
        config.credentials_path, config.drive_scopes, port=8080
    ).authenticate()
    drive_scanner = DriveMetaDataScanner(drive_creds)
    writer = MasterCsvMetaDataWriter()
    db_writer = MongoDbWriter()

    exporter = DriveMetaDataExporter(
        drive_scanner, writer, db_writer, drive_creds, config, None
    )

    file_name = f"drive_metadata.csv"
    output_csv = os.path.join("F:\\GFM Data\\drivemetadata\\", file_name)
    # drive_metadata_df = exporter.meta_data_writer()

    # exporter.export(drive_metadata_df, output_csv)
    exporter.run()
    exporter.verify()
    # exporter.export_to_db(output_csv, "driveMetaDataCollection")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Completed in {elapsed_time:.4f} seconds")
