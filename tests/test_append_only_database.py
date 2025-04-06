import csv
import logging
import tempfile
import time
from pathlib import Path

import pytest

from dumbdb.append_only_database import AppendOnlyDatabase


def test_init():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        assert db.name == "test_db"
        assert db.tables_dir == Path(temp_dir) / "test_db/tables"
        assert db.tables_dir.exists()


def test_get_table_file_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        assert db.get_table_file_path("users") == Path(
            temp_dir) / "test_db/tables/users.csv"


def test_create_table():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        assert db.get_table_file_path("users") == Path(
            temp_dir) / "test_db/tables/users.csv"
        assert db.get_table_file_path("users").exists()


def test_insert():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", {"id": "1", "name": "John Doe", "age": "20"})
        db.insert("users", {"id": "2", "name": "Jane Doe", "age": "21"})
        assert db.get_table_file_path("users").exists()

        with open(db.get_table_file_path("users"), "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[0] == ["id", "name", "age", "__deleted__"]
            assert rows[1] == ["1", "John Doe", "20", "False"]
            assert rows[2] == ["2", "Jane Doe", "21", "False"]


def test_update():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        db.update("users", {"id": "1", "name": "John Smith", "age": "21"})
        assert db.get_table_file_path("users").exists()

        with open(db.get_table_file_path("users"), "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[0] == ["id", "name", "age", "__deleted__"]
            assert rows[1] == ["1", "John Smith", "20", "False"]
            assert rows[2] == ["1", "John Smith", "21", "False"]


def test_update_non_existing_row():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        with pytest.raises(ValueError):
            db.update("users", {"id": "1", "name": "John Smith", "age": "21"})


def test_delete():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        db.delete("users", {"id": "1", "name": "John Smith", "age": "20"})
        assert db.get_table_file_path("users").exists()

        with open(db.get_table_file_path("users"), "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[0] == ["id", "name", "age", "__deleted__"]
            assert rows[1] == ["1", "John Smith", "20", "False"]
            assert rows[2] == ["1", "John Smith", "20", "True"]


def test_query():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        db.insert("users", {"id": "2", "name": "Jane Smith", "age": "21"})
        query_result = db.query("users", {"id": "1"})
        assert len(query_result.rows) == 1
        assert query_result.rows[0]["id"] == "1"
        assert query_result.rows[0]["name"] == "John Smith"
        assert query_result.rows[0]["age"] == "20"


def test_query_after_update():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        db.update("users", {"id": "1", "name": "John Smith", "age": "21"})
        query_result = db.query("users", {"id": "1"})
        assert len(query_result.rows) == 1
        assert query_result.rows[0]["id"] == "1"
        assert query_result.rows[0]["name"] == "John Smith"
        assert query_result.rows[0]["age"] == "21"


def test_query_after_delete():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        db.delete("users", {"id": "1", "name": "John Smith", "age": "20"})
        query_result = db.query("users", {"id": "1"})
        assert len(query_result.rows) == 0


def test_query_after_delete_and_reinsert():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        db.delete("users", {"id": "1", "name": "John Smith", "age": "20"})
        db.insert("users", {"id": "1", "name": "John Smith", "age": "22"})
        query_result = db.query("users", {"id": "1"})
        assert len(query_result.rows) == 1
        assert query_result.rows[0]["id"] == "1"
        assert query_result.rows[0]["name"] == "John Smith"
        assert query_result.rows[0]["age"] == "22"


def test_compact_table():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])

        db.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        db.insert("users", {"id": "2", "name": "Mike Smith", "age": "21"})
        db.insert("users", {"id": "3", "name": "Luke Skywalker", "age": "26"})

        db.update("users", {"id": "1", "name": "John Smith", "age": "21"})
        db.delete("users", {"id": "2", "name": "Mike Smith", "age": "21"})

        db.compact_table("users")
        assert db.get_table_file_path("users").exists()

        with open(db.get_table_file_path("users"), "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[0] == ["id", "name", "age", "__deleted__"]
            assert rows[1] == ["1", "John Smith", "21", "False"]
            assert rows[2] == ["3", "Luke Skywalker", "26", "False"]


def test_drop_table():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test_db", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])
        db.drop_table("users")
        assert not db.get_table_file_path("users").exists()

        with pytest.raises(ValueError):
            db.pretty_query("users", {"id": "1"})


def test_append_only_database_performance():
    with tempfile.TemporaryDirectory() as temp_dir:
        db = AppendOnlyDatabase(name="test", root_dir=Path(temp_dir))
        db.create_table("users", ["id", "name", "age"])

        num_rows = 10_000
        for i in range(num_rows//1000):
            logging.info(f"Current number of rows in the db: {i*1000}")
            start = time.time()
            for j in range(1000):
                db.insert(
                    "users", {"id": "1", "name": "John Doe", "age": str(j)})
            end = time.time()
            logging.info(
                f"Time taken to insert 1000 rows: {end-start:.4f} seconds")

        db.drop_table("users")
        db.create_table("users", ["id", "name", "age"])

        for i in range(num_rows//1000):
            for j in range(1000):
                db.insert(
                    "users", {"id": "1", "name": "John Doe", "age": str(j)})
            logging.info(f"Current number of rows in the db: {(i+1)*1000}")
            start = time.time()
            db.query("users", {"id": "1"})
            end = time.time()
            logging.info(
                f"Time taken to query 1 row: {end-start:.4f} seconds")

        db.drop_table("users")
        db.create_table("users", ["id", "name", "age"])

        # Insert data into the table
        for i in range(num_rows):
            db.insert("users", {"id": "1", "name": "John Doe", "age": str(i)})
            if i % 10000 == 0:
                query_result = db.query("users", {"id": "1"})
                logging.info(
                    f"Execution time with {i+1} rows: {query_result.time*1000: .4f} ms")

        db.compact_table("users")

        query_result = db.query("users", {"id": "1"})
        logging.info(
            f"Execution time with after compacting rows: {query_result.time*1000:.4f} ms")

        logging.info(
            f"Inserting {num_rows} different rows into the users table")

        # Insert data into the table
        for i in range(num_rows):
            db.insert("users", {"id": str(i),
                      "name": f"John Doe {i}", "age": str(i)})
            if i % 10000 == 0:
                query_result = db.query("users", {"id": "1"})
                logging.info(
                    f"Execution time with {i+1} rows: {query_result.time*1000: .4f} ms")

        query_result = db.query("users", {"id": "1"})
        logging.info(
            f"Execution time with after compacting rows: {query_result.time*1000:.4f} ms")
