import csv
import tempfile
from io import TextIOWrapper
from pathlib import Path

from dumbdb.hash_index import HashIndex


def test_hash_index_from_csv():
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_file_path = Path(temp_dir) / "test.csv"
        with open(csv_file_path, 'w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(["id", "name", "age"])
            writer.writerow(["1", "John Smith", "20"])
            writer.writerow(["2", "Jane Smith", "21"])
            writer.writerow(["3", "Jim Smith", "22"])
        index = HashIndex.from_csv(csv_file_path, "id")
        assert index.get_row_offsets("1") == (12, 28)
        assert index.get_row_offsets("2") == (28, 44)
        assert index.get_row_offsets("3") == (44, 59)

        with open(csv_file_path, 'rb') as f:
            f = TextIOWrapper(f, encoding='utf-8', newline='')
            f.seek(index.get_row_offsets("1")[0])
            assert f.read(index.get_row_offsets("1")[1] -
                          index.get_row_offsets("1")[0]) == "1,John Smith,20\n"
            f.seek(index.get_row_offsets("2")[0])
            assert f.read(index.get_row_offsets("2")[1] -
                          index.get_row_offsets("2")[0]) == "2,Jane Smith,21\n"
            f.seek(index.get_row_offsets("3")[0])
