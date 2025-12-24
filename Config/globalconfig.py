from dataclasses import dataclass
from typing import Optional


@dataclass
class GlobalConfig:
    drive_metadata_dir: str = "F:\\GFM Data\\drivemetadata\\"
    master_metadata_dir: str = "F:\\GFM Data\\master\\"
    raw_metadata_dir: str = "F:\\GFM Data\\metadata\\"

    # I/O
    log_path = "F:\\GFM Data\\logs"
    credentials_path = "F:\\GFM Data\\client_secret.json"
    drive_scopes = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
    output_dir: str = "F:\\GFM Data\\drivemetadata\\"
    base_filename: str = "drive_metadata"  # final file name pattern base
    compression: Optional[str] = None  # None or "gzip"
    write_header: bool = True  # header only for first chunk of the (rotated) file
    temp_suffix: str = ".tmp"
    checkpoint_filename: str = ".checkpoint.json"
    missing_ids: str = "missing_ids.txt"
    extra_ids: str = "extra_ids.txt"

    # Naming / rotation
    daily_naming: bool = True  # include YYYY-MM-DD in file name
    max_file_size_mb: int = 50  # rotate when exceeded
    start_batch_index: int = 1  # starting batch index for rotated files

    # Drive API
    batch_size: int = 1000  # files().list pageSize (<= 1000)
    fields: str = (
        "nextPageToken, files(id, name, originalFilename, webViewLink, mimeType, fileExtension, size, createdTime, modifiedTime)"
    )
    q: Optional[str] = None  # optional Drive query to filter
    order_by: Optional[str] = None  # e.g., "modifiedTime asc"

    # Resume / append
    resume: bool = True  # resume from checkpoint if exists
    append_skip_existing: bool = True  # avoid duplicates by last_id logic

    # Safety / rate
    permission_retries: int = 5
    permission_retry_sleep_sec: float = 1.5
    quota_sleep_sec: float = 0.3  # throttle between API calls

    # Logging
    log_every_n_pages: int = 1  # info log frequency

    # Schema
    drive_expected_columns = [
        "id",
        "name",
        "mimeType",
        "webViewLink",
        "createdTime",
        "modifiedTime",
        "originalFilename",
        "fileExtension",
        "size",
        "data_date",
    ]
