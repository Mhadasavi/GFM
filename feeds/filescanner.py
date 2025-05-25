import os
from datetime import datetime
from lib2to3.pytree import convert
from typing import List

from feeds.filemetadata import FileMetaData
from feeds.filemetadatautils import FileMetaDataUtils


class FileScanner:
    def __init__(self, folder_path: str):
        self.folder_path = folder_path

    def scan(self) -> List[FileMetaData]:
        metadata_list = []
        for root, _, files in os.walk(self.folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                stats = os.stat(file_path)
                size = FileMetaDataUtils.convert_size(stats.st_size)
                raw_size = size[:-2]
                size_unit = size[-2:]
                metadata = FileMetaData(
                    file,
                    file_path,
                    raw_size,
                    size_unit,
                    datetime.fromtimestamp(stats.st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
                    datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    datetime.fromtimestamp(stats.st_atime).strftime("%Y-%m-%d %H:%M:%S")
                )
                metadata_list.append(metadata)
        return metadata_list