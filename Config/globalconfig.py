from dataclasses import dataclass


@dataclass
class GlobalConfig:
    drive_metadata_dir: str = "F:\\GFM Data\\drivemetadata\\"
    master_metadata_dir: str = "F:\\GFM Data\\master\\"

    file_metadata_master_csv: str = "file_metadata_master.csv"
    drive_metadata_csv: str = "drive_metadata_2025-10-21_batch1.csv"
