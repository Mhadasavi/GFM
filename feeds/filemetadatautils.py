import logging
import re


class FileMetaDataUtils:

    file_type_dict = {
        "jpeg": "img",
        "jpg": "img",
        "png": "img",
        "gif": "img",
        "bmp": "img",
    }

    @staticmethod
    def convert_size(size_bytes):
        for unit in ["Bytes", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.2f}{unit}"
            size_bytes /= 1024

    @staticmethod
    def get_file_type(file_name: str) -> str | None:
        if not file_name:
            return None

        matcher = re.search("[^.]+$", file_name)

        if not matcher:
            return None

        extension = matcher.group(0).lower()
        return FileMetaDataUtils.file_type_dict.get(extension, extension)

    """Extracts the first 4-digit year from a given folder path string."""

    @staticmethod
    def get_year_from_path(folder: str) -> str | None:
        matcher = re.search(r"\d{4}", folder)
        if matcher:
            year = matcher.group(0)
            return year
        else:
            return None
