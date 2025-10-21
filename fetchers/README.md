# Google Drive Metadata Downloader — fetchers

A memory-safe, resumable tool to stream Google Drive file metadata to CSV files in chunks. Designed for long-running jobs, large accounts, and incremental exports.

---

## Table of contents
- [Key features](#key-features)
- [How it works](#how-it-works)
- [Configuration](#configuration)
- [Quick usage summary](#quick-usage-summary)
- [Folder structure](#folder-structure)
- [Component responsibilities](#component-responsibilities)
- [Future enhancements](#future-enhancements)
- [Example run summary](#example-run-summary)

---

## Key features
- Efficient chunked API reads (configurable page size; default 1000).
- Streamed CSV writes: append mode, header written once, no full dataset in memory.
- Automatic resume via checkpoint file (last processed file ID + pageToken).
- Incremental appending: skips already-processed IDs and appends only new records.
- File rotation & splitting: automatically rotates when CSV exceeds a configured size (e.g., 100 MB).
- Daily / batch-based naming: output files include date and batch index.
- Safe temporary writes: writes to `.tmp` then renames on success.
- Permission & file-lock handling: detects locked files and retries safely.
- API quota management: configurable delay between API calls to avoid throttling.
- Robust logging: logs fetches, pages, rotations, checkpoints, and errors.
- Cross-platform and interrupt-safe: resume without duplicates.
- Configurable & extensible architecture (separate classes, follows SOLID principles).
- Append-only mode for continuous incremental runs.

---

## How it works
1. Start / Resume
   - On start, looks for a checkpoint. If found, resumes from last saved state; otherwise, starts fresh.

2. Fetch in chunks
   - Uses Google Drive API with configurable `pageSize` (default 1000). Uses `nextPageToken` to paginate.
   - Adds a `quota_sleep` delay between calls to respect API quotas.

3. Stream to CSV
   - Each chunk is immediately appended to CSV (no in-memory accumulation).
   - Header is written once across runs when needed.

4. Checkpointing
   - After each page, updates checkpoint with last processed file ID and page token.
   - Allows exact resume without duplication or data loss.

5. File rotation
   - Continuously monitors CSV file size and creates a new batch file when it exceeds `max_file_size_mb`.
   - Filenames follow: `metadata_<YYYY-MM-DD>_batch<N>.csv`

6. Safe finalization
   - Writes to `<filename>.tmp` during the run, renames to `.csv` only after successful completion.

7. Logging & retries
   - Logs key events and retries on errors (file locked, transient API issues).

---

## Configuration

All settings are centralized in a `DownloaderConfig` object (can be a Python dataclass, JSON, or YAML). Example defaults:

```python
DownloaderConfig(
    output_dir="output/",
    batch_size=1000,            # pageSize for Drive list calls
    max_file_size_mb=100,       # rotate after 100 MB
    quota_sleep=0.5,            # seconds between API calls
    daily_naming=True,          # include date in filename
    checkpoint_path="output/checkpoint.json",
    append_mode=True,
    temp_suffix=".tmp",
    log_level="INFO",
    max_retries=3
)
```

Common parameters
- output_dir (str): Directory to save CSV and checkpoints.
- batch_size (int): Number of records per API call.
- max_file_size_mb (int): Maximum CSV size before rotation.
- quota_sleep (float): Delay between API calls.
- checkpoint_path (str): Where checkpoint state is stored.
- append_mode (bool): Append to existing CSVs instead of overwriting.
- temp_suffix (str): Temporary file suffix used during writes.
- max_retries (int): Retries for locked files or transient API errors.

---

## Quick usage summary
- Ensure `credentials.json` (Google API credentials) is available.
- Configure `DownloaderConfig` or settings file.
- Run the main entry point (typically `main.py`), which:
  - Initializes config and logging
  - Authenticates with Google Drive API
  - Instantiates the downloader and runs the fetch-write-checkpoint loop
- Interruptions are safe: re-running will resume from the checkpoint and append only new records.


---

## Component responsibilities
- main.py
  - Entry point: loads config, sets up logger, authenticates, runs downloader.

- downloader_config.py
  - Central place for configuration (defaults and overrides).

- drive_metadata_downloader.py
  - Core logic: list files with pagination, apply rate limiting and retries, update checkpoint after each page, call csv writer.

- csv_writer.py
  - Streaming CSV writes, append mode, file rotation, .tmp safety, skip already-processed IDs.

- checkpoint_manager.py
  - Reads/writes checkpoint state (last ID, pageToken, batch index).

- utils/logger_util.py
  - Central logging config including log rotation.

- utils/file_utils.py
  - Helpers: ensure directories, check sizes, safe rename, lock handling.

---

## Future enhancements (planned / optional)
- API and fetching
  - Google Sheets & Docs metadata
  - Incremental fetch by modifiedTime
  - Multi-account parallel fetching with safe throttling
  - Concurrent fetching with adaptive chunk sizes

- Storage & export
  - SQLite / Parquet output options
  - Compression for older batches
  - Cloud storage (GCS / S3 / Azure Blob) export

- Resilience & usability
  - Interactive CLI/dashboard
  - Alerts (Slack / email) and detailed error analytics
  - Encryption for checkpoints and secure token refresh

- Developer experience
  - Unit/integration tests, typed config (Pydantic), plugin-style data providers

---

## Example run summary
Step | Action | Output
--- | --- | ---
1 | First run | metadata_2025-10-21_batch1.csv
2 | File reaches threshold | metadata_2025-10-21_batch2.csv
3 | Script interrupted | Checkpoint saved (resume available)
4 | Resume next run | Continues from last saved ID / pageToken
5 | Finish run | `.tmp` file finalized and optionally renamed to `.csv`

---

## Notes & best practices
- Never commit `credentials.json` or any secrets to the repo.
- Set conservative `quota_sleep` and `max_retries` values to protect against API throttling.
- Use append mode for continuous syncs and daily batch naming for easier archival.
- Add integration / unit tests for checkpoint consistency and CSV rotation logic before scaling to multi-account runs.

---
