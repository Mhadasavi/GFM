from abc import ABC, abstractmethod
from typing import List

from pandas import DataFrame

from abstract.filemetadata import FileMetaData


class MetaDataWriter(ABC):
    @abstractmethod
    def write(self, data: List[FileMetaData] | dict, output_path: str):
        pass

    @abstractmethod
    def write_df_to_csv(self, drive_metadata_df: DataFrame, output_path: str):
        pass
