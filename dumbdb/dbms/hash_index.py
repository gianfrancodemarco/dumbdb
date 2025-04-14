import csv
from dataclasses import dataclass, field
from io import TextIOWrapper
from pathlib import Path


@dataclass
class HashIndex:
    __index__: dict[str, dict] = field(default_factory=dict)

    @property
    def n_keys(self) -> int:
        return len(self.__index__)

    def get_row_offsets(self, key: str) -> tuple[int, int]:
        """
        Get the starting and ending offset of the row in the file for a given key.
        """
        return self.__index__[key]

    def set_row_offsets(self, key: str, start_byte: int, end_byte: int):
        """
        Set the starting and ending offset of the row in the file for a given key.
        """
        self.__index__[key] = (start_byte, end_byte)

    def delete_row_offsets(self, key: str):
        """
        Delete a key from the hash index.
        """
        del self.__index__[key]

    @classmethod
    def from_csv(
        self,
        csv_file_path: Path,
        index_column: str = "id"
    ):
        """
        Return an instance of HashIndex built from a CSV file.
        The index uses the index_column value as the key and the starting and ending offset of the row in the file as the value.
        """
        index = HashIndex()
        with open(csv_file_path, 'rb') as f:

            # Wrap the binary file with a TextIOWrapper for proper decoding.
            f = TextIOWrapper(f, encoding='utf-8', newline='')

            # Read the header row and store it.
            header_line = f.readline()
            headers = next(csv.reader([header_line]))

            while True:
                # Record starting byte offset from the underlying binary buffer.
                start_byte = f.tell()
                line = f.readline()
                if not line:
                    break
                end_byte = f.tell()

                row_values = next(csv.reader([line]))
                row_dict = dict(zip(headers, row_values))

                if row_dict["__deleted__"] == "True":
                    index.delete_row_offsets(row_dict[index_column])
                else:
                    index.set_row_offsets(
                        row_dict[index_column], start_byte, end_byte)
        return index
