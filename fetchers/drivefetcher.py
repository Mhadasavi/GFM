# -----------------------------
# Drive Fetcher (single responsibility: paging)
# -----------------------------
import logging
import os
from typing import Dict, Iterable, Optional

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from Config.globalconfig import GlobalConfig
from fetchers.ratelimiter import RateLimiter


class DriveFetcher:
    def __init__(
        self,
        credentials: Credentials,
        cfg: GlobalConfig,
        # logger: logging.Logger,
        rate: RateLimiter,
    ):
        self.service = build("drive", "v3", credentials=credentials)
        self.cfg = cfg
        self.logger = logging.getLogger(__name__)
        self.rate = rate

    def fetch_page(self, page_token: Optional[str]) -> Dict:
        req = self.service.files().list(
            pageSize=self.cfg.batch_size,
            fields=self.cfg.fields,
            pageToken=page_token,
            q=self.cfg.q,
            orderBy=self.cfg.order_by,
        )
        self.logger.info("fetch_page invoked.")
        resp = req.execute()
        return resp

    def fetch_ids_from_page(self, page_token: Optional[str]) -> Dict:
        req = self.service.files().list(
            pageSize=self.cfg.batch_size,
            fields="nextPageToken, files(id)",
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        self.logger.info("fetch_ids_from_page invoked.")
        resp = req.execute()
        return resp

    def iterate_pages(self, start_token: Optional[str]) -> Iterable[Dict]:
        token = start_token
        while True:
            resp = self.fetch_page(token)
            yield resp
            token = resp.get("nextPageToken")
            if not token:
                break
            self.rate.sleep()

    def fetch_all_ids(self):
        """Fetch all file IDs via API — lightweight pagination."""
        ids = set()
        page_token = None

        while True:
            response = self.fetch_ids_from_page(page_token)

            for f in response.get("files", []):
                ids.add(f["id"])

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        self.logger.info(f"Fetched {len(ids)} total file IDs from Drive.")
        return ids

    def read_csv_ids(self):
        """Read unique IDs from all CSV batches."""
        ids = set()
        for file in os.listdir(self.cfg.output_dir):
            if file.endswith(".csv"):
                try:
                    df = pd.read_csv(
                        os.path.join(self.cfg.output_dir, file), usecols=["id"]
                    )
                    ids.update(df["id"].dropna().astype(str).unique())
                except Exception as e:
                    self.logger.warning(f"Skipping {file}: {e}")
        return ids
