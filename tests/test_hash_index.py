import csv
import tempfile
import pytest
from io import TextIOWrapper
from pathlib import Path

from dumbdb.dbms import HashIndex


def test_hash_index_from_csv():
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_file_path = Path(temp_dir) / "test.csv"
        with open(csv_file_path, 'w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(["id", "name", "age", "__deleted__"])
            writer.writerow(["1", "John Smith", "20", "False"])
            writer.writerow(["2", "Jane Smith", "21", "False"])
            writer.writerow(["3", "Jim Smith", "22", "False"])

            # update row with id 1
            writer.writerow(["1", "John Smith", "21", "False"])
            # delete row with id 2
            writer.writerow(["2", "Jane Smith", "21", "True"])

        index = HashIndex.from_csv(csv_file_path, "id")
        assert index.get_row_offsets("1") == (89, 111)
        with pytest.raises(KeyError):
            index.get_row_offsets("2")
        assert index.get_row_offsets("3") == (68, 89)

        with open(csv_file_path, 'rb') as f:
            f = TextIOWrapper(f, encoding='utf-8', newline='')
            f.seek(index.get_row_offsets("1")[0])
            assert f.read(index.get_row_offsets("1")[1] -
                          index.get_row_offsets("1")[0]) == "1,John Smith,21,False\n"
            f.seek(index.get_row_offsets("3")[0])
            assert f.read(index.get_row_offsets("3")[1] -
                          index.get_row_offsets("3")[0]) == "3,Jim Smith,22,False\n"


def test_n_keys_property():
    index = HashIndex()
    assert index.n_keys == 0

    index.set_row_offsets("key1", 0, 10)
    assert index.n_keys == 1

    index.set_row_offsets("key2", 10, 20)
    assert index.n_keys == 2


def test_set_row_offsets():
    index = HashIndex()
    index.set_row_offsets("key1", 0, 10)
    assert index.get_row_offsets("key1") == (0, 10)

    # Test overwriting existing key
    index.set_row_offsets("key1", 20, 30)
    assert index.get_row_offsets("key1") == (20, 30)


def test_get_row_offsets_key_error():
    index = HashIndex()
    with pytest.raises(KeyError):
        index.get_row_offsets("non_existent_key")


def test_delete_row_offsets():
    index = HashIndex()
    index.set_row_offsets("key1", 0, 10)
    assert index.get_row_offsets("key1") == (0, 10)
    index.delete_row_offsets("key1")
    with pytest.raises(KeyError):
        index.get_row_offsets("key1")


def test_from_csv_empty_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_file_path = Path(temp_dir) / "empty.csv"
        with open(csv_file_path, 'w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(["id", "name", "age"])

        index = HashIndex.from_csv(csv_file_path, "id")
        assert index.n_keys == 0


def test_from_csv_custom_index_column():
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_file_path = Path(temp_dir) / "custom_index.csv"
        with open(csv_file_path, 'w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(["id", "name", "age", "__deleted__"])
            writer.writerow(["1", "John Smith", "20", "False"])
            writer.writerow(["2", "Jane Smith", "21", "False"])

        # Index by name instead of id
        index = HashIndex.from_csv(csv_file_path, "name")
        assert "John Smith" in index.__index__
        assert "Jane Smith" in index.__index__


def test_from_csv_missing_index_column():
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_file_path = Path(temp_dir) / "missing_column.csv"
        with open(csv_file_path, 'w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(["id", "name", "age"])
            writer.writerow(["1", "John Smith", "20"])

        # Try to index by a column that doesn't exist
        with pytest.raises(KeyError):
            HashIndex.from_csv(csv_file_path, "non_existent_column")
