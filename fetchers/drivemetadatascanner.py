from typing import Dict, List

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from abstract.filemetadatautils import FileMetaDataUtils


class DriveMetaDataScanner:
    def __init__(self, credentials: Credentials):
        self.service = build("drive", "v3", credentials=credentials)

    @staticmethod
    def extract_size_and_unit(size):
        if pd.isnull(size):
            return 0.00, ""
        size_str = FileMetaDataUtils.convert_size(int(size))
        return float(size_str[:-2]), size_str[-2:]

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
            page_token = None
            if not page_token:
                break
        df = pd.DataFrame(all_files)

        # df['new_size'] = df['size'].apply(lambda x: FileMetaDataUtils.convert_size(int(x))[:-2] if pd.notnull(x) else 0.00)
        #
        # df['unit'] = df['new_size'].apply(lambda x:str(x)[-2:])

        # Extract size and unit in one pass
        df[["new_size", "unit"]] = df["size"].apply(
            lambda x: pd.Series(self.extract_size_and_unit(x))
        )

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
