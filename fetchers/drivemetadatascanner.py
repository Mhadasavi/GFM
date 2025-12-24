import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from abstract.filemetadatautils import FileMetaDataUtils


class DriveMetaDataScanner:
    def __init__(self, credentials: Credentials):
        self.service = build("drive", "v3", credentials=credentials)

    # def __init__(self, credentials: Credentials, cfg: Config, service, logger: logging.Logger):
    #     self.logger = logger
    #     self.service = build("drive", "v3", credentials=credentials)
    #     self.cfg = cfg
    #     self.cp = CheckpointManager(cfg, logger)
    #     self.writer = CSVChunkWriter(cfg, logger)
    #     self.rate = RateLimiter(cfg.quota_sleep_sec)
    #     self.fetcher = DriveFetcher(service, cfg, logger, self.rate)

    # @staticmethod
    # def extract_size_and_unit(size):
    #     if pd.isnull(size):
    #         return 0.00, ""
    #     size_str = FileMetaDataUtils.convert_size(int(size))
    #     return float(size_str[:-2]), size_str[-2:]

    # def _discover_resume_points(self) -> Tuple[Optional[str], Optional[str], int]:
    #     cp = self.cp.load()
    #     last_id = cp.get("last_id")
    #     page_token = cp.get("page_token")
    #     batch_index = cp.get("batch_index", self.writer.get_batch_index())
    #
    #     # If appending to an existing file without checkpoint, try last line for last_id
    #     if self.cfg.resume and not last_id and self.cfg.append_skip_existing:
    #         # read the *current* final file's last line
    #         current_final = self.writer.get_current_output_path()
    #         last_line = FileMetaDataUtils.read_last_line_text(current_final)
    #         if last_line:
    #             parsed = FileMetaDataUtils.extract_first_column_from_csv_line(last_line)
    #             # avoid treating header as ID
    #             if parsed and parsed != "id":
    #                 last_id = parsed
    #                 self.logger.info(f"Derived last_id from existing CSV: {last_id}")
    #
    #     return last_id, page_token, batch_index

    # def _skip_until_last_id(self, files: List[Dict], last_id: str) -> Tuple[List[Dict], bool]:
    #     """
    #     Skip items until last_id encountered. Returns (remaining_files, found).
    #     """
    #     for i, f in enumerate(files):
    #         if f.get("id") == last_id:
    #             return files[i + 1 :], True
    #     return files, False

    # def _save_checkpoint_safely(self, last_id: Optional[str], page_token: Optional[str]):
    #     # Include the current batch index so rotation persists across restarts
    #     self.cp.save(last_id=last_id, page_token=page_token, batch_index=self.writer.get_batch_index())

    def scan(self):
        all_files = []
        data = []
        page_token = None

        while True:
            # Prepare request parameters
            kwargs = {
                "pageSize": 1000,
                "fields": "nextPageToken, files(id, name, originalFilename, webViewLink, mimeType, fileExtension, size, createdTime, modifiedTime)",
            }
            if page_token:
                kwargs["pageToken"] = page_token

            # API call
            response = self.service.files().list(**kwargs).execute()
            items = response.get("files", [])

            all_files.extend(items)

            # Pagination
            page_token = response.get("nextPageToken")
            page_token = None  # remove this line once everything works as expected..............................
            if not page_token:
                break
        df = pd.DataFrame(all_files)

        # df['new_size'] = df['size'].apply(lambda x: FileMetaDataUtils.convert_size(int(x))[:-2] if pd.notnull(x) else 0.00)
        #
        # df['unit'] = df['new_size'].apply(lambda x:str(x)[-2:])

        # Extract size and unit in one pass
        # df[["new_size", "unit"]] = df["size"].apply(
        #     lambda x: pd.Series(self.extract_size_and_unit(x))
        # )

        hash_fields = [
            "id",
            "name",
            "originalFilename",
            "webViewLink",
            "mimeType",
            "fileExtension",
            "new_size",
            "unit",
            "createdTime",
            "modifiedTime",
        ]

        df["meta_row_id"] = df.apply(
            lambda row: FileMetaDataUtils.get_file_hash(
                [str(row[col]) for col in hash_fields]
            ),
            axis=1,
        )

        return df
