import csv
import logging
import tempfile
from pathlib import Path

import pytest

from dumbdb.dbms import AppendOnlyDBMS
from dumbdb.parser.ast import Column, EqualsCondition, AndCondition


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
        dbms.update("users", {"age": "21"},
                    EqualsCondition(Column("id"), "1"))
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
        dbms.delete("users", where_clause=(
            EqualsCondition(Column("id"), "1")))
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
        query_result = dbms.query(
            "users", EqualsCondition(Column("id"), "1"))
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
        dbms.update("users", {"age": "21"},
                    EqualsCondition(Column("id"), "1"))
        query_result = dbms.query("users", EqualsCondition(Column("id"), "1"))
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
        dbms.delete("users", where_clause=(
            EqualsCondition(Column("id"), "1")))
        query_result = dbms.query("users", EqualsCondition(Column("id"), "1"))
        assert len(query_result.rows) == 0


def test_query_after_delete_and_reinsert():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])
        dbms.insert("users", {"id": 1, "name": "John Smith", "age": 20})
        dbms.delete("users", where_clause=(
            EqualsCondition(Column("id"), "1")))
        dbms.insert("users", {"id": 1, "name": "John Smith", "age": 22})
        query_result = dbms.query("users", EqualsCondition(Column("id"), "1"))
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
                query_result = dbms.query(
                    "users", EqualsCondition(Column("id"), "1"))
                logging.info(
                    f"Execution time with {i+1} rows: {query_result.time*1000: .4f} ms")

        dbms.compact_table("users")

        query_result = dbms.query("users", EqualsCondition(Column("id"), "1"))
        logging.info(
            f"Execution time with after compacting rows: {query_result.time*1000:.4f} ms")

        logging.info(
            f"Inserting {num_rows} different rows into the users table")

        # Insert data into the table
        for i in range(num_rows):
            dbms.insert("users", {"id": i, "name": f"John Doe {i}", "age": i})
            if i % 10000 == 0:
                query_result = dbms.query(
                    "users", EqualsCondition(Column("id"), "1"))
                logging.info(
                    f"Execution time with {i+1} rows: {query_result.time*1000: .4f} ms")

        query_result = dbms.query(
            "users", EqualsCondition(Column("id"), "1"))
        logging.info(
            f"Execution time with after compacting rows: {query_result.time*1000:.4f} ms")


def test_use_database_with_nonexistent_database():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        with pytest.raises(ValueError, match="Database 'nonexistent_db' does not exist"):
            dbms.use_database("nonexistent_db")


def test_query_with_where_condition():
    """Test querying with WHERE conditions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane", "age": "21"})
        dbms.insert("users", {"id": "3", "name": "John", "age": "22"})

        # Test WHERE condition on id
        result = dbms.query("users", EqualsCondition(Column("id"), "1"))
        assert len(result.rows) == 1
        assert result.rows[0]["id"] == "1"
        assert result.rows[0]["name"] == "John"
        assert result.rows[0]["age"] == "20"

        # Test WHERE condition on name
        result = dbms.query("users", EqualsCondition(Column("name"), "'John'"))
        assert len(result.rows) == 2
        assert all(row["name"] == "John" for row in result.rows)

        # Test WHERE condition on age
        result = dbms.query("users", EqualsCondition(Column("age"), "21"))
        assert len(result.rows) == 1
        assert result.rows[0]["age"] == "21"
        assert result.rows[0]["name"] == "Jane"


def test_query_with_multiple_where_conditions():
    """Test querying with multiple WHERE conditions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "John", "age": "21"})
        dbms.insert("users", {"id": "3", "name": "Jane", "age": "20"})

        # Test multiple WHERE conditions
        where_clause = AndCondition(
            EqualsCondition(Column("name"), "'John'"),
            EqualsCondition(Column("age"), "20")
        )
        result = dbms.query("users", where_clause)
        assert len(result.rows) == 1
        assert result.rows[0]["id"] == "1"
        assert result.rows[0]["name"] == "John"
        assert result.rows[0]["age"] == "20"


def test_query_with_nonexistent_where_condition():
    """Test querying with WHERE conditions that don't match any rows."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane", "age": "21"})

        # Test WHERE condition that doesn't match any rows
        result = dbms.query("users", EqualsCondition(
            Column("name"), "'Alice'"))
        assert len(result.rows) == 0

        # Test multiple WHERE conditions that don't match any rows
        where_clause = AndCondition(
            EqualsCondition(Column("name"), "'John'"),
            EqualsCondition(Column("age"), "21")
        )
        result = dbms.query("users", where_clause)
        assert len(result.rows) == 0


def test_query_with_where_condition_after_update():
    """Test querying with WHERE conditions after updating rows."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane", "age": "21"})

        # Update a row
        dbms.update("users", {"age": "22"},
                    EqualsCondition(Column("id"), "1"))

        # Test WHERE condition after update
        result = dbms.query("users", EqualsCondition(Column("age"), "22"))
        assert len(result.rows) == 1
        assert result.rows[0]["id"] == "1"
        assert result.rows[0]["name"] == "John"
        assert result.rows[0]["age"] == "22"

        # Test WHERE condition that should match old value
        result = dbms.query("users", EqualsCondition(Column("age"), "20"))
        assert len(result.rows) == 0


def test_query_with_where_condition_after_delete():
    """Test querying with WHERE conditions after deleting rows."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane", "age": "21"})

        # Delete a row
        dbms.delete("users", where_clause=(
            EqualsCondition(Column("id"), "1")))

        # Test WHERE condition after delete
        result = dbms.query("users", EqualsCondition(Column("id"), "1"))
        assert len(result.rows) == 0

        # Test WHERE condition that should still match
        result = dbms.query("users", EqualsCondition(Column("id"), "2"))
        assert len(result.rows) == 1
        assert result.rows[0]["id"] == "2"
        assert result.rows[0]["name"] == "Jane"
        assert result.rows[0]["age"] == "21"


def test_update_with_where_condition():
    """Test updating rows with WHERE conditions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane", "age": "20"})
        dbms.insert("users", {"id": "3", "name": "Jim", "age": "25"})

        # Update all users with age 20
        dbms.update("users", {"age": "21"}, where_clause=(
            EqualsCondition(Column("age"), "20")))

        # Verify the updates
        result = dbms.query("users", EqualsCondition(Column("age"), "21"))
        assert len(result.rows) == 2
        assert any(row["name"] == "John" for row in result.rows)
        assert any(row["name"] == "Jane" for row in result.rows)

        # Verify unchanged row
        result = dbms.query("users", EqualsCondition(Column("age"), "25"))
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "Jim"


def test_delete_with_where_condition():
    """Test deleting rows with WHERE conditions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane", "age": "20"})
        dbms.insert("users", {"id": "3", "name": "Jim", "age": "25"})

        # Delete all users with age 20
        dbms.delete("users", where_clause=(
            EqualsCondition(Column("age"), "20")))

        # Verify the deletes
        result = dbms.query("users", EqualsCondition(Column("age"), "20"))
        assert len(result.rows) == 0

        # Verify unchanged row
        result = dbms.query("users", EqualsCondition(Column("age"), "25"))
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "Jim"


def test_update_with_complex_where_condition():
    """Test updating rows with complex WHERE conditions using AND."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMS(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "John", "age": "25"})
        dbms.insert("users", {"id": "3", "name": "Jane", "age": "20"})

        # Update users named John who are 20 years old
        where_clause = AndCondition(
            EqualsCondition(Column("name"), "John"),
            EqualsCondition(Column("age"), "20")
        )
        dbms.update("users", {"age": "21"}, where_clause)

        # Verify the update
        result = dbms.query("users", EqualsCondition(Column("age"), "21"))
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "John"
        assert result.rows[0]["id"] == "1"

        # Verify unchanged rows
        result = dbms.query("users", EqualsCondition(Column("age"), "20"))
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "Jane"

        result = dbms.query("users", EqualsCondition(Column("age"), "25"))
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "John"
        assert result.rows[0]["id"] == "2"
