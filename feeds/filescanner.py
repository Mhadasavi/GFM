import os
from datetime import datetime

from abstract.filemetadata import FileMetaData
from abstract.filemetadatautils import FileMetaDataUtils


class FileScanner:
    def __init__(self, folder_path: str):
        self.folder_path = folder_path

    def scan(self) -> tuple[str | None, list[FileMetaData]]:
        metadata_list = []
        file_type = None

        def format_time(ts):
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

        for root, _, files in os.walk(self.folder_path):
            for file in files:
                if file_type is None:
                    file_type = FileMetaDataUtils.get_file_type(file)
                file_path = os.path.join(root, file)
                stats = os.stat(file_path)
                size = FileMetaDataUtils.convert_size(stats.st_size)
                raw_size = size[:-2]
                size_unit = size[-2:]
                created = format_time(stats.st_ctime)
                modified = format_time(stats.st_mtime)
                accessed = format_time(stats.st_atime)
                hash_fields = [
                    file,
                    file_path,
                    raw_size,
                    size_unit,
                    created,
                    modified,
                    accessed,
                ]
                meta_row_id = FileMetaDataUtils.get_file_hash(hash_fields)
                metadata = FileMetaData(
                    file,
                    file_path,
                    raw_size,
                    size_unit,
                    created,
                    modified,
                    accessed,
                    meta_row_id,
                )
                metadata_list.append(metadata)
        return file_type, metadata_list
