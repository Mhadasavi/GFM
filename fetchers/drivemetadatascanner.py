from typing import Dict, List

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


class DriveMetaDataScanner:
    def __init__(self, credentials: Credentials):
        self.service = build("drive", "v3", credentials=credentials)

    # def scan(self)-> List[Dict]:
    #     files = []
    #     page_token = None,
    #     while True:
    #         # kwargs = {
    #         #     'pageSize': 1000,
    #         #     'fields': "nextPageToken, files(id, name, mimeType, createdTime, size, modifiedTime, owners)"
    #         # }
    #         # if page_token:
    #         #     kwargs['pageToken'] = page_token  # ✅ only add if not None
    #         # file_service = self.service.files()
    #         # # file_service.list(pageSize=10, fields="files(id, name, mimeType, createdTime, size, modifiedTime, owners)").execute()
    #         # response = file_service.list(**kwargs).execute()
    #         # Call the Drive v3 API
    #         results = self.service.files().list(pageSize=1000,
    #                                        fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)").execute()
    #         # get the results
    #         items = results.get('files', [])
    #         files.extend(response.get('files', []))
    #         page_token = response.get('nextPageToken')
    #         if not page_token:
    #             break
    #     return files

    def scan(self):
        all_files = []
        data = []
        page_token = None

        while True:
            # Prepare request parameters
            kwargs = {"pageSize": 1000, "fields": "nextPageToken, files(*)"}
            if page_token:
                kwargs["pageToken"] = page_token

            # API call
            response = self.service.files().list(**kwargs).execute()
            items = response.get("files", [])

            # # Process each file
            # for row in items:
            #     if row.get("mimeType") != "application/vnd.google-apps.folder":
            #         row_data = []
            #         try:
            #             size_mb = round(int(row["size"]) / 1_000_000, 2)
            #         except KeyError:
            #             size_mb = 0.00
            #         row_data.append(size_mb)
            #         row_data.append(row.get("id", ""))
            #         row_data.append(row.get("name", ""))
            #         row_data.append(row.get("modifiedTime", ""))
            #         row_data.append(row.get("mimeType", ""))
            #         data.append(row_data)

            all_files.extend(items)

            # Pagination
            page_token = response.get("nextPageToken")
            if not page_token:
                break

        df = pd.DataFrame(all_files)
        return df
