# -----------------------------
# Drive Fetcher (single responsibility: paging)
# -----------------------------
import logging
from typing import Dict, Iterable, Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from fetchers.Config import DownloaderConfig
from fetchers.ratelimiter import RateLimiter


class DriveFetcher:
    def __init__(
        self,
        credentials: Credentials,
        cfg: DownloaderConfig,
        logger: logging.Logger,
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

    def iterate_pages(self, start_token: Optional[str]) -> Iterable[Dict]:
        token = start_token
        while True:
            resp = self.fetch_page(token)
            yield resp
            token = resp.get("nextPageToken")
            if not token:
                break
            self.rate.sleep()
