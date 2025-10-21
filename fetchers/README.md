🚀 Features
1. Efficient, Chunked API Reading

Fetches Google Drive metadata in configurable page sizes (default: 1000).

Prevents memory overload by stream-processing each page instead of loading all data at once.

2. Stream Writing to CSV

Writes results incrementally as they’re fetched — no need to hold the entire dataset in memory.

Uses append mode for continuous writing.

Writes the CSV header only once, even across multiple runs.

3. Automatic Resume & Checkpoints

Maintains a checkpoint file storing the last processed file ID and pageToken.

Can resume exactly from where it stopped in case of network failure, crash, or interruption.

Ensures no duplicates or data loss between runs.

4. Incremental Appending

Detects if a CSV already exists and appends only new records.

Skips already-processed IDs automatically.

Ideal for daily incremental updates or continuous metadata syncing.

5. File Rotation & Splitting

Monitors CSV size and automatically starts a new batch file when it exceeds a configurable threshold (e.g., 100 MB).

File names include batch numbers, e.g.:

metadata_2025-10-21_batch1.csv
metadata_2025-10-21_batch2.csv

6. Daily / Batch-Based Naming

Automatically names output files based on the current date and batch index.

Keeps daily exports organized and easy to archive or process later.

7. Safe Temporary Writes

Writes data first to a temporary .tmp file, then renames it only after a successful write.

Prevents file corruption if the program crashes mid-write.

8. Permission & File Lock Handling

Detects when the output file is open or locked.

Waits and retries safely instead of crashing with a PermissionError.

9. API Quota Management

Adds a configurable delay between API calls to respect Google Drive API rate limits.

Prevents accidental throttling or userRateLimitExceeded errors.

10. Robust Logging

Logs every step: fetch start/end, page number, file rotation, resume point, and errors.

Simplifies debugging and long-run monitoring.

11. Memory-Safe, Long-Run Friendly

Designed for large Google Drive accounts with thousands of files.

Uses constant memory footprint even for millions of rows.

12. Configurable & Extensible Design

Built following SOLID principles:

Separate classes for API fetching, CSV writing, and checkpoint management.

Easy to extend (e.g., support Parquet, SQLite, or cloud storage outputs later).

All options configurable:

DownloaderConfig(
    output_dir="output/",
    batch_size=1000,
    max_file_size_mb=100,
    quota_sleep=0.5,
    daily_naming=True
)

13. Cross-Platform & Fail-Safe

Works on Windows, Linux, and macOS.

Safe to interrupt anytime — can resume later without re-fetching old data.

14. Append-Only Mode

Supports continuous runs (daily/hourly syncs) without wiping old data.

Ideal for metadata version tracking or incremental ETL pipelines.

⚙️ How It Works

This tool connects to the Google Drive API, fetches metadata in small, memory-efficient chunks, and writes it to CSV files safely and incrementally.

🧩 Detailed Breakdown
1. Start / Resume Run

When the program starts, it checks for an existing checkpoint.

If found, it resumes exactly where it left off.

If not found, it starts fresh.

2. Fetch in Chunks

The Google Drive API is queried with a configurable pageSize (default: 1000).

Each API response includes a nextPageToken, which is used to fetch the next chunk.

A short delay (quota_sleep) is added between calls to respect rate limits.

3. Stream to CSV

Each chunk is written immediately to the CSV file (no full DataFrame in memory).

The header is written only once.

Uses append mode (a) for continuous writing.

4. Checkpointing

After every page, the script updates the checkpoint file with:

The last processed file ID.

The current pageToken.

If the process stops unexpectedly, it can restart from this exact point.

5. File Rotation

The CSV file size is monitored continuously.

If it exceeds the configured threshold (e.g., 100 MB), a new batch file is created.

File naming pattern:

metadata_2025-10-21_batch1.csv
metadata_2025-10-21_batch2.csv

6. Safe Finalization

Data is first written to a temporary file (.tmp).

Once the batch finishes successfully, the temp file is renamed to .csv.

Prevents corruption in case of an incomplete run.

7. Logging

Every key event is logged:

Fetch start/end per page

Checkpoint updates

File rotations

Errors & retries

🔁 On Next Run

When you run the program again:

It reads the checkpoint.

Skips already written IDs.

Resumes from the next unread page.

Appends only new data to the CSV file (or next batch file).

🧠 Example Run Summary
Step	Action	Output
1	First run	metadata_2025-10-21_batch1.csv
2	File reaches 100MB	metadata_2025-10-21_batch2.csv
3	Script interrupted	Checkpoint saved
4	Resume next day	Continues from last saved ID
5	Finish run	.tmp renamed safely to .csv


⚙️ Configuration & Usage
1️⃣ Configuration Parameters

All configuration options are stored in a DownloaderConfig object (or JSON/YAML if you prefer).
Each parameter is fully customizable.

Parameter	Type	Default	Description
output_dir	str	"output/"	Directory where CSV files will be saved. Created automatically if missing.
batch_size	int	1000	Number of records fetched per API call (pageSize).
max_file_size_mb	int	100	Maximum size (in MB) of each CSV file before rotation.
quota_sleep	float	0.5	Delay (in seconds) between API calls to respect Google Drive API quotas.
daily_naming	bool	True	If True, output files are named with the current date.
checkpoint_path	str	"output/checkpoint.json"	Path to checkpoint file (stores progress for resume).
append_mode	bool	True	If True, appends new data to existing CSV instead of overwriting.
temp_suffix	str	".tmp"	Temporary file suffix for safe write operations.
log_level	str	"INFO"	Logging level (DEBUG / INFO / WARNING / ERROR).
max_retries	int	3	Number of retry attempts when file is locked or API temporarily unavailable.

📁 Folder Structure & Responsibilities
📦 GoogleDrive-Metadata-Downloader
│
├── main.py                              # Entry point: initializes config, service, and runs downloader
│
├── downloader_config.py                 # Defines DownloaderConfig (all settings & parameters)
│
├── drive_metadata_downloader.py         # Core downloader logic — fetch, write, and checkpoint management
│
├── csv_writer.py                        # Handles CSV writing, file rotation, append mode, and temp file safety
│
├── checkpoint_manager.py                # Manages progress checkpointing for safe resume
│
├── utils/
│   ├── logger_util.py                   # Centralized logger configuration and log rotation setup
│   └── file_utils.py                    # Helper functions: file size check, safe writes, and folder creation
│
├── credentials.json                     # Google API credentials (never commit publicly)
│
├── README.md                            # Project documentation
└── requirements.txt                     # Required dependencies

🧩 Component Responsibilities
1️⃣ main.py

Orchestrates the entire program flow.

Loads configuration, sets up the logger, and authenticates Google Drive.

Instantiates and executes the downloader.

2️⃣ downloader_config.py

Centralizes all configurable parameters (output path, batch size, etc.).

Allows overriding defaults via code or config files.

Ensures consistency across components.

3️⃣ drive_metadata_downloader.py

The heart of the project.

Fetches file metadata from Google Drive API using pagination.

Calls the CSV Writer for output and updates checkpoint after each batch.

Handles API errors, quota delays, and retry logic.

4️⃣ csv_writer.py

Writes Drive metadata to CSV in append or rotation mode.

Automatically splits files when size exceeds limit.

Uses .tmp suffix for safe writes and renames on completion.

5️⃣ checkpoint_manager.py

Saves the progress (last processed file ID, pageToken, etc.).

Allows resume from the last checkpoint.

Prevents duplicate records when rerunning.

6️⃣ utils/logger_util.py

Provides a preconfigured logging setup with timestamps and levels.

Supports file-based and console logging.

Handles log rotation to prevent oversized log files.

7️⃣ utils/file_utils.py

Ensures output directories exist.

Validates file sizes and naming conventions.

Contains safe-write utilities and path helpers.

🚀 Future Enhancements

This project is designed with extensibility in mind — making it easy to add new data sources, optimizations, and cloud integrations over time.
Below are planned and potential future enhancements.

🧠 1. API Enhancements

Google Sheets & Docs Metadata Support – Extend current logic to include metadata for other Google Workspace files.

Incremental Fetch Mode – Fetch only new or modified files since last run, using the Drive modifiedTime field.

Multi-Account Parallel Fetching – Allow simultaneous metadata extraction from multiple Google accounts using parallel threads or async requests.

💾 2. Storage & Output Improvements

SQLite/Parquet Export – Option to export metadata to SQLite or Parquet for better analytics performance.

Automatic Compression – Compress older CSV batches into .zip or .gz to save space.

Cloud Storage Integration – Push metadata directly to Google Cloud Storage, AWS S3, or Azure Blob.

🧩 3. Performance & Resilience

Concurrent API Fetching – Split file listing into parallel threads (safe throttling to avoid 403 errors).

Adaptive Chunk Size – Dynamically adjust API page size based on response time and quota usage.

Memory-Aware Mode – Detect available memory and reduce chunk size automatically.

🧰 4. Usability & Monitoring

Interactive CLI Dashboard – Real-time progress bar, speed metrics, and batch summaries.

Detailed Error Analytics – Separate log files for API errors, permissions, and rate-limit events.

Email/Slack Alerts – Notify on job completion or failure.

🔐 5. Security & Reliability

Encrypted Checkpoints – Encrypt checkpoint data for sensitive environments.

OAuth Token Refresh Handling – Automatic token renewal for long-running jobs.

Transaction-based Writes – Full atomicity: checkpoint updates only after successful CSV writes.

🌙 6. Automation & Scheduling

Daily/Weekly Scheduler – Built-in cron-compatible runner to execute automatically.

Batch Retention Policy – Auto-delete or archive metadata older than N days.

Incremental File Naming – Add date-based versioning for easier historical tracking.

🧪 7. Developer Experience

Unit & Integration Tests – Add pytest coverage for fetch, write, and resume logic.

Typed Config (Pydantic) – Strong schema validation for configuration parameters.

Plug-in Architecture – Define new “Data Providers” (e.g., Gmail, Drive, Sheets) using a shared interface.

🪄 8. Long-Term Vision

Unified Google Data Backup Tool
→ Integrate Drive, Gmail, Photos, and Calendar into a single backup & metadata extraction suite.

GUI Version (PyQt / Tkinter)
→ Allow non-technical users to select folders, view progress, and download reports visually.

Dockerized Deployment
→ Run the downloader in a container with minimal setup on any environment.