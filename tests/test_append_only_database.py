import csv
import logging
import tempfile
from pathlib import Path

import pytest

from dumbdb.append_only_dbms import AppendOnlyDBMS


def test_init():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        assert dbms.current_database == "test_db"
        assert dbms.tables_dir == Path(temp_dir) / "test_db/tables"
        assert dbms.tables_dir.exists()


def test_create_database():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        assert dbms.get_database_dir("test_db") == Path(temp_dir) / "test_db"
        assert dbms.get_database_dir("test_db").exists()


def test_use_database():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        assert dbms.current_database == "test_db"
        assert dbms.tables_dir == Path(temp_dir) / "test_db/tables"
        assert dbms.tables_dir.exists()


def test_create_table_without_use_database():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        with pytest.raises(ValueError):
            dbms.create_table("users", ["id", "name", "age"])

        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        assert dbms.get_table_file_path("users") == Path(
            temp_dir) / "test_db/tables/users.csv"
        assert dbms.get_table_file_path("users").exists()


def test_get_table_file_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        assert dbms.get_table_file_path("users") == Path(
            temp_dir) / "test_db/tables/users.csv"


def test_create_table():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        assert dbms.get_table_file_path("users") == Path(
            temp_dir) / "test_db/tables/users.csv"
        assert dbms.get_table_file_path("users").exists()


def test_insert():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        dbms.insert("users", {"id": "1", "name": "John Doe", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane Doe", "age": "21"})
        assert dbms.get_table_file_path("users").exists()

        with open(dbms.get_table_file_path("users"), "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[0] == ["id", "name", "age", "__deleted__"]
            assert rows[1] == ["1", "John Doe", "20", "False"]
            assert rows[2] == ["2", "Jane Doe", "21", "False"]


def test_update():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        dbms.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        dbms.update("users", {"id": "1", "name": "John Smith", "age": "21"})
        assert dbms.get_table_file_path("users").exists()

        with open(dbms.get_table_file_path("users"), "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[0] == ["id", "name", "age", "__deleted__"]
            assert rows[1] == ["1", "John Smith", "20", "False"]
            assert rows[2] == ["1", "John Smith", "21", "False"]


def test_delete():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        dbms.insert("users", {"id": 1, "name": "John Smith", "age": 20})
        dbms.delete("users", {"id": 1, "name": "John Smith", "age": 20})
        assert dbms.get_table_file_path("users").exists()

        with open(dbms.get_table_file_path("users"), "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[0] == ["id", "name", "age", "__deleted__"]
            assert rows[1] == ["1", "John Smith", "20", "False"]
            assert rows[2] == ["1", "John Smith", "20", "True"]


def test_query():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        dbms.insert("users", {"id": 1, "name": "John Smith", "age": 20})
        dbms.insert("users", {"id": 2, "name": "Jane Smith", "age": 21})
        query_result = dbms.query("users", {"id": "1"})
        assert len(query_result.rows) == 1
        assert query_result.rows[0]["id"] == "1"
        assert query_result.rows[0]["name"] == "John Smith"
        assert query_result.rows[0]["age"] == "20"


def test_query_after_update():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        dbms.insert("users", {"id": "1", "name": "John Smith", "age": "20"})
        dbms.update("users", {"id": "1", "name": "John Smith", "age": "21"})
        query_result = dbms.query("users", {"id": "1"})
        assert len(query_result.rows) == 1
        assert query_result.rows[0]["id"] == "1"
        assert query_result.rows[0]["name"] == "John Smith"
        assert query_result.rows[0]["age"] == "21"


def test_query_after_delete():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        dbms.insert("users", {"id": 1, "name": "John Smith", "age": 20})
        dbms.delete("users", {"id": 1, "name": "John Smith", "age": 20})
        query_result = dbms.query("users", {"id": "1"})
        assert len(query_result.rows) == 0


def test_query_after_delete_and_reinsert():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        dbms.insert("users", {"id": 1, "name": "John Smith", "age": 20})
        dbms.delete("users", {"id": 1, "name": "John Smith", "age": 20})
        dbms.insert("users", {"id": 1, "name": "John Smith", "age": 22})
        query_result = dbms.query("users", {"id": "1"})
        assert len(query_result.rows) == 1
        assert query_result.rows[0]["id"] == "1"
        assert query_result.rows[0]["name"] == "John Smith"
        assert query_result.rows[0]["age"] == "22"


def test_append_only_database():
    # Create a database
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test")
        dbms.use_database("test")

        # Create a table with specific headers
        try:
            users_table = dbms.create_table("users", ["id", "name", "age"])
            logging.info(f"Created table: {users_table}")
        except ValueError as e:
            logging.error(e)

        num_rows = 100_000
        logging.info(
            f"Inserting {num_rows} identical rows into the users table")

        # Insert data into the table
        for i in range(num_rows):
            dbms.insert("users", {"id": 1, "name": "John Doe", "age": i})
            if i % 10000 == 0:
                query_result = dbms.query("users", {"id": 1})
                logging.info(
                    f"Execution time with {i+1} rows: {query_result.time*1000: .4f} ms")

        dbms.compact_table("users")

        query_result = dbms.query("users", {"id": 1})
        logging.info(
            f"Execution time with after compacting rows: {query_result.time*1000:.4f} ms")

        logging.info(
            f"Inserting {num_rows} different rows into the users table")

        # Insert data into the table
        for i in range(num_rows):
            dbms.insert("users", {"id": i, "name": f"John Doe {i}", "age": i})
            if i % 10000 == 0:
                query_result = dbms.query("users", {"id": 1})
                logging.info(
                    f"Execution time with {i+1} rows: {query_result.time*1000: .4f} ms")

        query_result = dbms.query("users", {"id": 1})
        logging.info(
            f"Execution time with after compacting rows: {query_result.time*1000:.4f} ms")


def test_use_database_with_nonexistent_database():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        with pytest.raises(ValueError, match="Database 'nonexistent_db' does not exist"):
            dbms.use_database("nonexistent_db")
