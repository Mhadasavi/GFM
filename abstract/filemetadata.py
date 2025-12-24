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
        data_date: str,
        meta_row_id: str,
    ):
        self.file_name = file_name
        self.path = path
        self.size = size
        self.unit = unit
        self.created = created
        self.modified = modified
        self.accessed = accessed
        self.data_date = data_date
        self.meta_row_id = meta_row_id

    def to_dict(self) -> Dict:
        return {
            "file_name": self.file_name,
            "path": self.path,
            "size": self.size,
            "unit": self.unit,
            "created": self.created,
            "modified": self.modified,
            "accessed": self.accessed,
            "data_date": self.data_date,
            "meta_row_id": self.meta_row_id,
        }
