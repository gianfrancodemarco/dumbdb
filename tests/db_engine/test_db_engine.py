from unittest.mock import Mock, patch

import pytest

from dumbdb.db_engine import DBEngine, Executor, QueryResult
from dumbdb.dbms.dbms import DBMS
from dumbdb.parser.ast import (Column, CreateDatabaseQuery, CreateTableQuery,
                               InsertQuery, SelectQuery, Table,
                               UseDatabaseQuery)


def test_query_result():
    """Test QueryResult class initialization and properties."""
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    result = QueryResult(rows)
    assert result.rows == rows


def test_executor_create_database_query():
    """Test Executor handling of CREATE DATABASE queries."""
    mock_dbms = Mock(spec=DBMS)
    executor = Executor(mock_dbms)
    query = CreateDatabaseQuery(database="my_database")


def test_executor_use_database_query():
    """Test Executor handling of USE DATABASE queries."""
    mock_dbms = Mock(spec=DBMS)
    executor = Executor(mock_dbms)
    query = UseDatabaseQuery(database="my_database")

    # Execute query
    result = executor.execute_query(query)

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []


def test_executor_create_table_query():
    """Test Executor handling of CREATE TABLE queries."""
    mock_dbms = Mock(spec=DBMS)
    executor = Executor(mock_dbms)
    query = CreateTableQuery(table=Table("my_table"), columns=[
                             Column("id"), Column("name")])

    # Execute query
    result = executor.execute_query(query)

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.create_table.assert_called_once_with("my_table", [
        Column("id"), Column("name")])


def test_executor_select_query():
    """Test Executor handling of SELECT queries."""
    # Create mock DBMS
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.query.return_value = QueryResult([{"id": 1, "name": "Alice"}])

    # Create executor and test query
    executor = Executor(mock_dbms)
    query = SelectQuery(columns=[Column("*")],
                        table=Table("users"), where_clause=None)

    # Execute query
    result = executor.execute_select_query(query)

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == [{"id": 1, "name": "Alice"}]
    mock_dbms.query.assert_called_once_with("users", {})


def test_executor_insert_query():
    """Test Executor handling of INSERT queries."""
    # Create mock DBMS
    mock_dbms = Mock(spec=DBMS)

    # Create executor and test query
    executor = Executor(mock_dbms)
    query = InsertQuery(
        table=Table("users"),
        columns=["id", "name"],
        values=["1", "'Alice'"]
    )

    # Execute query
    result = executor.execute_insert_query(query)

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.insert.assert_called_once_with(
        "users", {"id": "1", "name": "'Alice'"})


def test_executor_unknown_query_type():
    """Test Executor handling of unknown query types."""
    mock_dbms = Mock(spec=DBMS)
    executor = Executor(mock_dbms)

    class UnknownQuery:
        pass

    with pytest.raises(Exception) as exc_info:
        executor.execute_query(UnknownQuery())
    assert "Query type" in str(exc_info.value)


def test_db_engine_execute_create_database():
    """Test DBEngine execution of CREATE DATABASE queries."""
    # Create mock components
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.create_database.return_value = True
    engine = DBEngine(dbms=mock_dbms)

    # Execute query
    result = engine.execute_query("CREATE DATABASE my_database;")

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.create_database.assert_called_once_with("my_database")


def test_db_engine_execute_select():
    """Test DBEngine execution of SELECT queries."""
    # Create mock components
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.query.return_value = QueryResult([{"id": 1, "name": "Alice"}])

    # Create DBEngine with mock components
    engine = DBEngine(dbms=mock_dbms)

    # Execute query
    result = engine.execute_query("SELECT * FROM users;")

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == [{"id": 1, "name": "Alice"}]


def test_db_engine_execute_insert():
    """Test DBEngine execution of INSERT queries."""
    # Create mock components
    mock_dbms = Mock(spec=DBMS)

    # Create DBEngine with mock components
    engine = DBEngine(dbms=mock_dbms)

    # Execute query
    result = engine.execute_query(
        "INSERT INTO users (id, name) VALUES (1, 'Alice');")

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.insert.assert_called_once_with(
        "users", {"id": "1", "name": "'Alice'"})


def test_db_engine_invalid_query():
    """Test DBEngine handling of invalid queries."""
    engine = DBEngine()

    with pytest.raises(Exception) as exc_info:
        engine.execute_query("INVALID QUERY;")
    assert "Invalid syntax" in str(exc_info.value)
