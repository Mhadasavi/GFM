import logging
import os
from doctest import master

import pandas as pd
from pandas import DataFrame

from Config.globalconfig import GlobalConfig


class CompareFeedsAndDrive:

    def __init__(self, cfg: GlobalConfig):
        self.logger = logging.getLogger(__name__)
        self.cfg = cfg

    def read_metadata(self, path: str, cols: []) -> DataFrame | None:
        df = None
        # input_path = os.path.join("F:\\GFM Data\\metadata\\", "file_metadata_master.csv")
        for file in os.listdir(path):
            if file.endswith(".csv"):
                try:
                    df = pd.read_csv(os.path.join(path, file), usecols=cols)
                except Exception as e:
                    self.logger.warning(f"Skipping {file}: {e}")
        #             i have to check if 2 columns of df1 are exist in df2, store them in a csv file.
        return df

    def compare_master_to_drive(
        self, master_df: DataFrame, drive_df: DataFrame, master_cols: [], drive_cols: []
    ):
        # create composite key
        master_df["_composite_key"] = (
            master_df[master_cols].astype(str).agg("_".join, axis=1)
        )
        drive_df["_composite_key"] = (
            drive_df[drive_cols].astype(str).agg("_".join, axis=1)
        )

        duplicates_df = master_df[
            master_df["_composite_key"].isin(drive_df["_composite_key"])
        ]
        unique_master_df = master_df[
            ~master_df["_composite_key"].isin(drive_df["_composite_key"])
        ]
        unique_drive_df = drive_df[
            ~drive_df["_composite_key"].isin(master_df["_composite_key"])
        ]

        for df in (duplicates_df, unique_drive_df, unique_master_df):
            df.drop(columns=["_composite_key"], inplace=True, errors="ignore")

        duplicates_df.to_csv(
            os.path.join("F:\\GFM Data\\", "duplicates.csv"), index=False
        )
        unique_master_df.to_csv(
            os.path.join("F:\\GFM Data\\", "unique_master_file.csv"), index=False
        )
        unique_drive_df.to_csv(
            os.path.join("F:\\GFM Data\\", "unique_drive_file.csv"), index=False
        )

    def run(self):
        master_input_path = (
            self.cfg.master_metadata_dir
        )  # os.path.join(self.cfg.master_metadata_dir, self.cfg.file_metadata_master_csv)
        master_cols = ["file_name", "size", "unit", "created"]
        # implement getfilename here based on regex
        drive_input_path = (
            self.cfg.drive_metadata_dir
        )  # os.path.join(self.cfg.drive_metadata_dir, self.cfg.drive_metadata_csv)
        drive_cols = ["name", "size", "fileExtension", "createdTime"]

        master_df = self.read_metadata(master_input_path, master_cols)
        drive_df = self.read_metadata(drive_input_path, drive_cols)
        self.compare_master_to_drive(master_df, drive_df, master_cols, drive_cols)


if __name__ == "__main__":
    compare = CompareFeedsAndDrive(GlobalConfig())
    compare.run()
