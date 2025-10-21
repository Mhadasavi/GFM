import json
import logging
import os
from typing import Dict, Optional

from abstract.filemetadatautils import FileMetaDataUtils
from fetchers.Config import DownloaderConfig


class CheckpointManager:
    def __init__(self, cfg: DownloaderConfig, logger: logging.Logger):
        self.cfg = cfg
        self.logger = logging.getLogger(__name__)
        self.path = os.path.join(cfg.output_dir, cfg.checkpoint_filename)
        FileMetaDataUtils.ensure_dir(cfg.output_dir)

    def load(self) -> Dict[str, Optional[str]]:
        if not self.cfg.resume:
            return {
                "last_id": None,
                "page_token": None,
                "batch_index": self.cfg.start_batch_index,
            }

        if not os.path.exists(self.path):
            return {
                "last_id": None,
                "page_token": None,
                "batch_index": self.cfg.start_batch_index,
            }

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # minimal validation
            return {
                "last_id": data.get("last_id"),
                "page_token": data.get("page_token"),
                "batch_index": int(data.get("batch_index", self.cfg.start_batch_index)),
            }
        except Exception as e:
            self.logger.warning(f"Failed to load checkpoint: {e}. Starting fresh.")
            return {
                "last_id": None,
                "page_token": None,
                "batch_index": self.cfg.start_batch_index,
            }

    def save(self, last_id: Optional[str], page_token: Optional[str], batch_index: int):
        data = {
            "last_id": last_id,
            "page_token": page_token,
            "batch_index": batch_index,
        }
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, self.path)
            self.logger.info("Checkpoint successfully written.")
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass
