from abc import ABC, abstractmethod
from typing import List

from feeds.filemetadata import FileMetaData


class MetaDataWriter(ABC):
    @abstractmethod
    def write(self, data: List[FileMetaData], output_path: str):
        pass
