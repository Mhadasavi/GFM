from typing import Dict


class FileMetaData:
    def __init__(
        self,
        file_name: str,
        path: str,
        size: str,
        unit: str,
        created: str,
        modified: str,
        accessed: str,
    ):
        self.file_name = file_name
        self.path = path
        self.size = size
        self.unit = unit
        self.created = created
        self.modified = modified
        self.accessed = accessed

    def to_dict(self) -> Dict:
        return {
            "file_name": self.file_name,
            "path": self.path,
            "size": self.size,
            "unit": self.unit,
            "created": self.created,
            "modified": self.modified,
            "accessed": self.accessed,
        }
