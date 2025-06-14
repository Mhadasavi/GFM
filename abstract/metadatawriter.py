from abc import ABC, abstractmethod
from typing import List

from abstract.filemetadata import FileMetaData


class MetaDataWriter(ABC):
    @abstractmethod
    def write(self, data: List[FileMetaData] | dict, output_path: str):
        pass
