#
# # -----------------------------
# # Orchestrator
# # -----------------------------
# class GoogleDriveMetadataDownloader:
#     """
#     - Chunked API reading
#     - Stream writing to CSV
#     - Resume from last ID (and checkpoint)
#     - Checkpoint files
#     - Write after every page
#     - File rotation / splitting
#     - Daily/Batch naming
#     - Temporary file + rename (chunk temp + atomic replace)
#     - Permission handling (retries)
#     - API quota respect (rate limiter)
#     - Logging
#     - Append to existing metadata (skip via last_id)
#     """
#
#     def __init__(self, cfg: DownloaderConfig, service, logger: logging.Logger):
#         self.cfg = cfg
#         self.logger = logger
#         self.cp = CheckpointManager(cfg, logger)
#         self.writer = CSVChunkWriter(cfg, logger)
#         self.rate = RateLimiter(cfg.quota_sleep_sec)
#         self.fetcher = DriveFetcher(service, cfg, logger, self.rate)
#
#     def _discover_resume_points(self) -> Tuple[Optional[str], Optional[str], int]:
#         cp = self.cp.load()
#         last_id = cp.get("last_id")
#         page_token = cp.get("page_token")
#         batch_index = cp.get("batch_index", self.writer.get_batch_index())
#
#         # If appending to an existing file without checkpoint, try last line for last_id
#         if self.cfg.resume and not last_id and self.cfg.append_skip_existing:
#             # read the *current* final file's last line
#             current_final = self.writer.get_current_output_path()
#             last_line = read_last_line_text(current_final)
#             if last_line:
#                 parsed = extract_first_column_from_csv_line(last_line)
#                 # avoid treating header as ID
#                 if parsed and parsed != "id":
#                     last_id = parsed
#                     self.logger.info(f"Derived last_id from existing CSV: {last_id}")
#
#         return last_id, page_token, batch_index
#
#     def _skip_until_last_id(self, files: List[Dict], last_id: str) -> Tuple[List[Dict], bool]:
#         """
#         Skip items until last_id encountered. Returns (remaining_files, found).
#         """
#         for i, f in enumerate(files):
#             if f.get("id") == last_id:
#                 return files[i + 1 :], True
#         return files, False
#
#     def run(self):
#         last_id, page_token, batch_index = self._discover_resume_points()
#
#         # Ensure writer batch matches checkpoint (e.g., after rotation).
#         if batch_index != self.writer.get_batch_index():
#             # Fast-forward writer's batch index by recreating with correct starting index.
#             self.writer.batch_index = batch_index
#             self.writer.final_path, self.writer.tmp_path = make_paths(self.cfg, batch_index)
#             self.writer.header_written_in_this_file = os.path.exists(self.writer.final_path)
#
#         page_counter = 0
#         for resp in self.fetcher.iterate_pages(start_token=page_token):
#             page_counter += 1
#             files = resp.get("files", []) or []
#
#             # If resuming, skip until we pass last_id once
#             if last_id and self.cfg.append_skip_existing:
#                 files, found = self._skip_until_last_id(files, last_id)
#                 if found:
#                     # we consumed the page that had last_id; subsequent pages are full
#                     last_id = None
#                 else:
#                     # still haven't found last_id; continue to next page
#                     self._save_checkpoint_safely(last_id=last_id, page_token=resp.get("nextPageToken"))
#                     if page_counter % self.cfg.log_every_n_pages == 0:
#                         self.logger.info(f"Skipping page {page_counter} (still searching last_id).")
#                      continue
#
#             # Write-after-every-page (stream)
#             if files:
#                 self.writer.write_chunk(files)
#                 last_written_id = files[-1].get("id")
#             else:
#                 last_written_id = last_id  # no progress on this page
#
#             # Save checkpoint *after* each write to minimize data loss
#             self._save_checkpoint_safely(last_id=last_written_id, page_token=resp.get("nextPageToken"))
#
#             if page_counter % self.cfg.log_every_n_pages == 0:
#                 self.logger.info(
#                     f"Wrote page {page_counter} | rows={len(files)} | "
#                     f"current_file={self.writer.get_current_output_path()} | "
#                     f"size={file_size_mb(self.writer.get_current_output_path()):.2f}MB"
#                 )
#
#         self.logger.info("Download complete.")
#
#     def _save_checkpoint_safely(self, last_id: Optional[str], page_token: Optional[str]):
#         # Include the current batch index so rotation persists across restarts
#         self.cp.save(last_id=last_id, page_token=page_token, batch_index=self.writer.get_batch_index())
