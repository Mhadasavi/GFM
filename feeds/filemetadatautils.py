class FileMetaDataUtils:
    @staticmethod
    def convert_size(size_bytes):
        for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f'{size_bytes:.2f}{unit}'
            size_bytes /= 1024
