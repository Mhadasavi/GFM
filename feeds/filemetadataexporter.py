import os.path
from importlib.metadata import metadata
from os import write
from shutil import which

from feeds.csvmetadatawriter import CSVMetaDataWriter
from feeds.filescanner import FileScanner
from feeds.metadatawriter import MetaDataWriter


class FileMetaDataExporter:
    def __init__(self, scanner: FileScanner, writer: MetaDataWriter):
        self.scanner = scanner
        self.writer = writer

    def export(self, output_path: str):
        metadata = self.scanner.scan()
        self.writer.write(metadata, output_path)


if __name__ == '__main__':
    folder = os.path.expanduser("H:\Premier Pro\Tutorials")
    output_csv = os.path.join(folder, "file_metadata.csv")

    scanner = FileScanner(folder)
    writer = CSVMetaDataWriter()

    exporter = FileMetaDataExporter(scanner, writer)
    exporter.export(output_csv)
