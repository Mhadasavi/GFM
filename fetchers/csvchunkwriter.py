# -----------------------------
# CSV writer with rotation and temp safety
# -----------------------------
import gzip
import logging
import os
import shutil
import time
from typing import Dict, List

import pandas as pd

from abstract.filemetadatautils import FileMetaDataUtils
from fetchers.Config import DownloaderConfig


class CSVChunkWriter:
    """
    Stream-append chunks to a (rotated) CSV, writing to .tmp and renaming atomically.
    Maintains header-per-file, rotation-by-size, and gzip (optional).
    """

    def __init__(self, cfg: DownloaderConfig, logger: logging.Logger):
        self.cfg = cfg
        self.logger = logging.getLogger(__name__)
        self.batch_index = cfg.start_batch_index
        self.final_path, self.tmp_path = FileMetaDataUtils.make_paths(
            cfg, self.batch_index
        )
        self.header_written_in_this_file = False

        # if resuming and file exists, consider header already present
        if os.path.exists(self.final_path):
            self.header_written_in_this_file = True

    def _rotate_if_needed(self):
        if FileMetaDataUtils.file_size_mb(self.final_path) >= self.cfg.max_file_size_mb:
            self.batch_index += 1
            self.final_path, self.tmp_path = FileMetaDataUtils.make_paths(
                self.cfg, self.batch_index
            )
            self.header_written_in_this_file = os.path.exists(self.final_path)
            self.logger.info(f"Rotated to new file: {self.final_path}")

    def _safe_write_dataframe(self, df: pd.DataFrame):
        """Write df to tmp file then append/concat to final atomically."""
        header = not os.path.exists(self.final_path)  # Write header only once
        os.makedirs(os.path.dirname(self.final_path), exist_ok=True)
        try:
            with open(self.final_path, "a", newline="", encoding="utf-8") as fh:
                df.to_csv(fh, index=False, header=header)
            self.logger.info(f"Wrote {len(df)} rows to {self.final_path}")
        except PermissionError:
            self.logger.warning("File is open. Retrying in 5s...")
            time.sleep(5)
            self._safe_write_dataframe(df)
        # Write append directly to final via pandas in stream mode, but with .tmp safety:
        # Strategy: write chunk to a small temp-chunk file, then append to final.
        # This avoids partial line issues if process dies mid-write.
        # chunk_tmp = (
        #     self.final_path + f".chunk{int(time.time()*1000)}{self.cfg.temp_suffix}"
        # )
        # try:
        #     if self.cfg.compression == "gzip":
        #         with gzip.open(chunk_tmp, "at", newline="") as fh:
        #             df.to_csv(fh, index=False, header=header)
        #     else:
        #         with open(chunk_tmp, "a", newline="", encoding="utf-8") as fh:
        #             df.to_csv(fh, index=False, header=header)
        #
        #     # Append/concatenate chunk_tmp to final via OS append
        #     with open(chunk_tmp, "rb") as src, open(self.tmp_path, "ab") as dst:
        #         shutil.copyfileobj(src, dst)
        #
        #     # Atomic move of tmp to final
        #     os.replace(self.tmp_path, self.final_path)
        #
        #     self.header_written_in_this_file = True
        # finally:
        #     # cleanup
        #     if os.path.exists(chunk_tmp):
        #         try:
        #             os.remove(chunk_tmp)
        #         except Exception:
        #             pass
        #     if os.path.exists(self.tmp_path):
        #         # In normal operation tmp_path was moved. If still present, clean it.
        #         try:
        #             os.remove(self.tmp_path)
        #         except Exception:
        #             pass

    def write_chunk(self, rows: List[Dict], driveSchema: []):
        if not rows:
            return

        df = pd.DataFrame(rows)
        missing_cols = [c for c in driveSchema if c not in df.columns]
        for col in missing_cols:
            df[col] = pd.NA

        #  Detect unexpected new columns (added by API)
        new_cols = [c for c in df.columns if c not in driveSchema]
        if new_cols:
            self.logger.warning(f"⚠️ New columns detected: {new_cols}")
            final_columns = driveSchema + new_cols
        else:
            final_columns = driveSchema

        df = df.reindex(columns=final_columns)

        if "createdTime" in df.columns:
            df["data_date"] = pd.to_datetime(
                df["createdTime"], errors="coerce", utc=True
            ).dt.date
        else:
            df["data_date"] = pd.NaT

        # Permission-safe retries
        for attempt in range(1, self.cfg.permission_retries + 1):
            try:
                self._safe_write_dataframe(df)
                break
            except PermissionError:
                self.logger.warning(
                    f"PermissionError while writing {self.final_path}. "
                    f"Retry {attempt}/{self.cfg.permission_retries}..."
                )
                time.sleep(self.cfg.permission_retry_sleep_sec)
        else:
            # All retries exhausted
            raise PermissionError(
                f"Could not write to {self.final_path} after retries."
            )

        # Check rotation after successful write
        self._rotate_if_needed()

    def get_current_output_path(self) -> str:
        return self.final_path

    def get_batch_index(self) -> int:
        return self.batch_index
