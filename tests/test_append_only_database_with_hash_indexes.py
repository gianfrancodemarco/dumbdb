import logging
import tempfile
from pathlib import Path

import pytest

from dumbdb.dbms import AppendOnlyDBMSWithHashIndexes
from dumbdb.parser.ast import AndCondition, Column, EqualsCondition


def test_use_db_creates_hash_indexes():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("test_table")
        assert dbms.hash_indexes["test_table"] is not None

        dbms.insert("test_table", {"id": "1", "name": "John"})
        dbms.insert("test_table", {"id": "2", "name": "Jane"})
        dbms.insert("test_table", {"id": "3", "name": "Jim"})

        # Let's reset the dbms instance and reload the database
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.use_database("test_db")

        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (16, 30)
        assert dbms.hash_indexes["test_table"].get_row_offsets("2") == (30, 44)
        assert dbms.hash_indexes["test_table"].get_row_offsets("3") == (44, 57)


def test_create_table_creates_hash_indexes():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("test_table", ["id", "name", "age"])
        assert dbms.hash_indexes["test_table"] is not None


def test_drop_table_deletes_hash_indexes():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("test_table", ["id", "name", "age"])
        assert dbms.hash_indexes["test_table"] is not None

        dbms.drop_table("test_table")
        assert "test_table" not in dbms.hash_indexes


def test_insert_adds_entry_to_hash_indexes():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("test_table", ["id", "name", "age"])
        assert dbms.hash_indexes["test_table"] is not None
        assert dbms.hash_indexes["test_table"].n_keys == 0
        dbms.insert("test_table", {"id": "1", "name": "John", "age": 20})
        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (25, 42)

        dbms.insert("test_table", {"id": "2", "name": "Jane"})
        assert dbms.hash_indexes["test_table"].get_row_offsets("2") == (42, 56)

        assert dbms.hash_indexes["test_table"].n_keys == 2


def test_update_modifies_entry_in_hash_indexes():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("test_table", ["id", "name", "age"])

        assert dbms.hash_indexes["test_table"].n_keys == 0

        dbms.insert("test_table", {"id": "1", "name": "John", "age": 20})
        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (25, 42)

        assert dbms.hash_indexes["test_table"].n_keys == 1

        dbms.update("test_table", {"id": "1", "name": "John", "age": 21})
        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (42, 59)

        assert dbms.hash_indexes["test_table"].n_keys == 1


def test_query_by_id_uses_hash_index():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("test_table", ["id", "name", "age"])
        dbms.insert("test_table", {"id": "1", "name": "John", "age": 20})
        dbms.insert("test_table", {"id": "2", "name": "Jane", "age": 21})
        dbms.insert("test_table", {"id": "3", "name": "Jim", "age": 22})

        assert dbms.query("test_table", EqualsCondition(Column("id"), "1")).rows == [
            {"id": "1", "name": "John", "age": "20"}]

        assert dbms.query("test_table", EqualsCondition(Column("id"), "2")).rows == [
            {"id": "2", "name": "Jane", "age": "21"}]

        assert dbms.query("test_table", EqualsCondition(Column("id"), "3")).rows == [
            {"id": "3", "name": "Jim", "age": "22"}]


def test_delete_deletes_entry_from_hash_index():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("test_table", ["id", "name", "age"])
        dbms.insert("test_table", {"id": "1", "name": "John", "age": 20})
        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (25, 42)
        dbms.delete("test_table", {"id": "1"})
        with pytest.raises(KeyError):
            assert dbms.hash_indexes["test_table"].get_row_offsets("1")

        dbms.insert("test_table", {"id": "1", "name": "John", "age": 20})
        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (50, 67)


def test_index_after_compaction():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("test_table", ["id", "name", "age"])

        dbms.insert("test_table", {"id": "1", "name": "John", "age": 20})
        dbms.insert("test_table", {"id": "2", "name": "Jane", "age": 21})
        dbms.insert("test_table", {"id": "3", "name": "Jim", "age": 22})

        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (25, 42)
        assert dbms.hash_indexes["test_table"].get_row_offsets("2") == (42, 59)
        assert dbms.hash_indexes["test_table"].get_row_offsets("3") == (59, 75)

        dbms.delete("test_table", {"id": "2", "name": "Jane", "age": 21})
        dbms.update("test_table", {"id": "3", "name": "Jim", "age": 23})

        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (25, 42)
        with pytest.raises(KeyError):
            assert dbms.hash_indexes["test_table"].get_row_offsets("2")
        assert dbms.hash_indexes["test_table"].get_row_offsets(
            "3") == (91, 107)

        dbms.compact_table("test_table")

        assert dbms.hash_indexes["test_table"].get_row_offsets("1") == (25, 42)
        with pytest.raises(KeyError):
            assert dbms.hash_indexes["test_table"].get_row_offsets("2")
        assert dbms.hash_indexes["test_table"].get_row_offsets("3") == (42, 58)


def test_query_by_id_performance():
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        num_rows = 100_000
        for i in range(num_rows):
            dbms.insert(
                "users", {"id": "1", "name": "John Doe", "age": str(i)})
            if i % 10000 == 0:
                query_result = dbms.query(
                    "users", EqualsCondition(Column("id"), "1"))
                logging.info(
                    f"Execution time with {i+1} rows: {query_result.time*1000: .4f} ms")
                assert query_result.rows == [
                    {"id": "1", "name": "John Doe", "age": str(i)}]


def test_query_with_where_condition():
    """Test querying with WHERE conditions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
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
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
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
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
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
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane", "age": "21"})

        # Update a row
        dbms.update("users", {"id": "1", "name": "John", "age": "22"})

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
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=Path(temp_dir))
        dbms.create_database("test_db")
        dbms.use_database("test_db")
        dbms.create_table("users", ["id", "name", "age"])

        # Insert test data
        dbms.insert("users", {"id": "1", "name": "John", "age": "20"})
        dbms.insert("users", {"id": "2", "name": "Jane", "age": "21"})

        # Delete a row
        dbms.delete("users", {"id": "1", "name": "John", "age": "20"})

        # Test WHERE condition after delete
        result = dbms.query("users", EqualsCondition(Column("id"), "1"))
        assert len(result.rows) == 0

        # Test WHERE condition that should still match
        result = dbms.query("users", EqualsCondition(Column("id"), "2"))
        assert len(result.rows) == 1
        assert result.rows[0]["id"] == "2"
        assert result.rows[0]["name"] == "Jane"
        assert result.rows[0]["age"] == "21"
