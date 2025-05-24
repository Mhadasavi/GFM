from datetime import datetime
from typing import Dict


class FileMetaData:
    def __init__(self, file_name: str, path: str, size: str, created: str, modified: str, accessed: str):
        self.file_name = file_name
        self.path = path
        self.size = size
        self.created = created
        self.modified = modified
        self.accessed = accessed

    def to_dict(self)->Dict:
        return {
            "file_name": self.file_name,
            "path": self.path,
            "size": self.size,
            "created": self.created,
            "modified": self.modified,
            "accessed": self.accessed
        }