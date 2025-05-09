import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from dumbdb.db_engine import DBEngine, Executor, QueryResult
from dumbdb.dbms.append_only_dbms import AppendOnlyDBMS
from dumbdb.dbms.dbms import DBMS
from dumbdb.parser.ast import (Column, CreateDatabaseQuery, CreateTableQuery,
                               InsertQuery, SelectQuery, Table,
                               UseDatabaseQuery, UpdateQuery, EqualsCondition)


def test_query_result():
    """Test QueryResult class initialization and properties."""
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    result = QueryResult(rows)
    assert result.rows == rows


def test_executor_create_database_query():
    """Test Executor handling of CREATE DATABASE queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.create_database.return_value = QueryResult()
    executor = Executor(mock_dbms)
    query = CreateDatabaseQuery(database="my_database")

    # Execute query
    result = executor.execute_query(query)

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.create_database.assert_called_once_with("my_database")


def test_executor_use_database_query():
    """Test Executor handling of USE DATABASE queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.use_database.return_value = QueryResult()
    executor = Executor(mock_dbms)
    query = UseDatabaseQuery(database="my_database")

    # Execute query
    result = executor.execute_query(query)

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.use_database.assert_called_once_with("my_database")


def test_executor_create_table_query():
    """Test Executor handling of CREATE TABLE queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.create_table.return_value = QueryResult()
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
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.query.return_value = QueryResult([{"id": 1, "name": "Alice"}])
    executor = Executor(mock_dbms)
    query = SelectQuery(
        columns=[Column("id"), Column("name")],
        table=Table("my_table")
    )

    # Execute query
    result = executor.execute_query(query)

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == [{"id": 1, "name": "Alice"}]
    mock_dbms.query.assert_called_once_with("my_table", {}, None)


def test_executor_insert_query():
    """Test Executor handling of INSERT queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.insert.return_value = QueryResult()
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


def test_executor_update_query():
    """Test Executor handling of UPDATE queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.update.return_value = QueryResult()
    executor = Executor(mock_dbms)
    query = UpdateQuery(
        table=Table("users"),
        set_clause={"name": "'John'", "age": "25"},
        where_clause=None
    )

    # Execute query
    result = executor.execute_update_query(query)

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.update.assert_called_once_with(
        "users", {"name": "'John'", "age": "25"}, None)


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
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.create_database.return_value = QueryResult()
    engine = DBEngine(dbms=mock_dbms)

    # Execute query
    result = engine.execute_query("CREATE DATABASE my_database;")

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.create_database.assert_called_once_with("my_database")


def test_db_engine_execute_select():
    """Test DBEngine execution of SELECT queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.query.return_value = QueryResult([{"id": 1, "name": "Alice"}])
    engine = DBEngine(dbms=mock_dbms)

    # Execute query
    result = engine.execute_query("SELECT * FROM users;")

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == [{"id": 1, "name": "Alice"}]
    mock_dbms.query.assert_called_once_with("users", {}, None)


def test_db_engine_execute_insert():
    """Test DBEngine execution of INSERT queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_dbms.insert.return_value = QueryResult()
    engine = DBEngine(dbms=mock_dbms)

    # Execute query
    result = engine.execute_query(
        "INSERT INTO users (id, name) VALUES (1, 'Alice');")

    # Verify results
    assert isinstance(result, QueryResult)
    assert result.rows == []
    mock_dbms.insert.assert_called_once_with(
        "users", {"id": "1", "name": "'Alice'"})


def test_db_engine_execute_update():
    """Test DBEngine execution of UPDATE queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_parser = Mock()
    mock_tokenizer = Mock()

    # Mock the tokenizer and parser
    with patch('dumbdb.db_engine.Tokenizer') as mock_tokenizer_class:
        mock_tokenizer_class.return_value = mock_tokenizer
        mock_tokenizer.tokenize.return_value = [
            'UPDATE', 'users', 'SET', 'name', '=', "'John'", ';'
        ]
        mock_parser.parse.return_value = UpdateQuery(
            table=Table("users"),
            set_clause={"name": "'John'"},
            where_clause=None
        )

        # Create engine with mocked components
        engine = DBEngine(dbms=mock_dbms, parser=mock_parser)
        mock_dbms.update.return_value = QueryResult()

        # Execute query
        result = engine.execute_query("UPDATE users SET name = 'John';")

        # Verify results
        assert isinstance(result, QueryResult)
        assert result.rows == []
        mock_tokenizer.tokenize.assert_called_once_with(
            "UPDATE users SET name = 'John';")
        mock_parser.parse.assert_called_once()
        mock_dbms.update.assert_called_once_with(
            "users", {"name": "'John'"}, None)


def test_db_engine_execute_update_with_where():
    """Test DBEngine execution of UPDATE queries with WHERE clause."""
    mock_dbms = Mock(spec=DBMS)
    mock_parser = Mock()
    mock_tokenizer = Mock()

    # Mock the tokenizer and parser
    with patch('dumbdb.db_engine.Tokenizer') as mock_tokenizer_class:
        mock_tokenizer_class.return_value = mock_tokenizer
        mock_tokenizer.tokenize.return_value = [
            'UPDATE', 'users', 'SET', 'name', '=', "'John'",
            'WHERE', 'id', '=', '1', ';'
        ]
        mock_parser.parse.return_value = UpdateQuery(
            table=Table("users"),
            set_clause={"name": "'John'"},
            where_clause=EqualsCondition(Column("id"), "1")
        )

        # Create engine with mocked components
        engine = DBEngine(dbms=mock_dbms, parser=mock_parser)
        mock_dbms.update.return_value = QueryResult()

        # Execute query
        result = engine.execute_query(
            "UPDATE users SET name = 'John' WHERE id = 1;")

        # Verify results
        assert isinstance(result, QueryResult)
        assert result.rows == []
        mock_tokenizer.tokenize.assert_called_once_with(
            "UPDATE users SET name = 'John' WHERE id = 1;")
        mock_parser.parse.assert_called_once()
        mock_dbms.update.assert_called_once_with(
            "users", {"name": "'John'"}, EqualsCondition(Column("id"), "1"))


def test_db_engine_invalid_query():
    """Test DBEngine handling of invalid queries."""
    mock_dbms = Mock(spec=DBMS)
    engine = DBEngine(dbms=mock_dbms)

    with pytest.raises(Exception) as exc_info:
        engine.execute_query("INVALID QUERY;")
    assert "Invalid syntax" in str(exc_info.value)


def test_db_engine_execute_query():
    """Test DBEngine execute_query method with various query types."""
    mock_dbms = Mock(spec=DBMS)
    mock_parser = Mock()
    mock_tokenizer = Mock()

    # Mock the tokenizer and parser
    with patch('dumbdb.db_engine.Tokenizer') as mock_tokenizer_class:
        mock_tokenizer_class.return_value = mock_tokenizer
        mock_tokenizer.tokenize.return_value = [
            'SELECT', '*', 'FROM', 'users', ';']
        mock_parser.parse.return_value = SelectQuery(
            columns=[Column("*")],
            table=Table("users")
        )

        # Create engine with mocked components
        engine = DBEngine(dbms=mock_dbms, parser=mock_parser)
        mock_dbms.query.return_value = QueryResult(
            [{"id": 1, "name": "Alice"}])

        # Execute query
        result = engine.execute_query("SELECT * FROM users;")

        # Verify results
        assert isinstance(result, QueryResult)
        assert result.rows == [{"id": 1, "name": "Alice"}]
        mock_tokenizer.tokenize.assert_called_once_with("SELECT * FROM users;")
        mock_parser.parse.assert_called_once()
        mock_dbms.query.assert_called_once_with("users", {}, None)


def test_db_engine_execute_query_invalid():
    """Test DBEngine execute_query method with invalid query."""
    mock_dbms = Mock(spec=DBMS)
    mock_parser = Mock()
    mock_tokenizer = Mock()

    # Mock the tokenizer and parser
    with patch('dumbdb.db_engine.Tokenizer') as mock_tokenizer_class:
        mock_tokenizer_class.return_value = mock_tokenizer
        mock_tokenizer.tokenize.return_value = ['INVALID', 'QUERY', ';']
        mock_parser.parse.return_value = None

        # Create engine with mocked components
        engine = DBEngine(dbms=mock_dbms, parser=mock_parser)

        # Execute query and verify exception
        with pytest.raises(Exception) as exc_info:
            engine.execute_query("INVALID QUERY;")
        assert "Invalid query" in str(exc_info.value)


def test_db_engine_execute_script():
    """Test DBEngine execute_script method with multiple queries."""
    mock_dbms = Mock(spec=DBMS)
    mock_parser = Mock()
    mock_tokenizer = Mock()

    # Mock the tokenizer and parser
    with patch('dumbdb.db_engine.Tokenizer') as mock_tokenizer_class:
        mock_tokenizer_class.return_value = mock_tokenizer

        # Setup mock responses for different queries
        def mock_tokenize(query):
            mock_tokenizer.tokenize.return_value = query.split()
            return mock_tokenizer.tokenize.return_value

        def mock_parse(tokens):
            if "SELECT" in tokens:
                mock_parser.parse.return_value = SelectQuery(
                    columns=[Column("*")],
                    table=Table("users")
                )
            elif "INSERT" in tokens:
                mock_parser.parse.return_value = InsertQuery(
                    table=Table("users"),
                    columns=["id", "name"],
                    values=["1", "'Alice'"]
                )
            return mock_parser.parse.return_value

        mock_tokenizer.tokenize.side_effect = mock_tokenize
        mock_parser.parse.side_effect = mock_parse

        # Create engine with mocked components
        engine = DBEngine(dbms=mock_dbms, parser=mock_parser)
        mock_dbms.query.return_value = QueryResult(
            [{"id": 1, "name": "Alice"}])
        mock_dbms.insert.return_value = QueryResult()

        # Execute script
        script = """
        SELECT * FROM users;
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        """
        engine.execute_script(script)

        # Verify results
        assert mock_tokenizer.tokenize.call_count == 2
        assert mock_parser.parse.call_count == 2
        mock_dbms.query.assert_called_once()
        mock_dbms.insert.assert_called_once()


def test_db_engine_cli():
    """Test DBEngine CLI method."""
    mock_dbms = Mock(spec=DBMS)
    mock_parser = Mock()
    mock_tokenizer = Mock()

    # Mock the tokenizer and parser
    with patch('dumbdb.db_engine.Tokenizer') as mock_tokenizer_class, \
            patch('builtins.input') as mock_input, \
            patch('builtins.print') as mock_print:

        mock_tokenizer_class.return_value = mock_tokenizer
        mock_tokenizer.tokenize.return_value = [
            'SELECT', '*', 'FROM', 'users', ';']
        mock_parser.parse.return_value = SelectQuery(
            columns=[Column("*")],
            table=Table("users")
        )

        # Setup mock input to return a query and then 'exit'
        mock_input.side_effect = ["SELECT * FROM users;", "exit"]

        # Create engine with mocked components
        engine = DBEngine(dbms=mock_dbms, parser=mock_parser)
        mock_dbms.query.return_value = QueryResult(
            [{"id": 1, "name": "Alice"}])

        # Run CLI
        engine.cli()

        # Verify results
        assert mock_input.call_count == 2
        assert mock_tokenizer.tokenize.call_count == 1
        assert mock_parser.parse.call_count == 1
        mock_dbms.query.assert_called_once()
        mock_print.assert_called()


def test_db_engine_cli_error_handling():
    """Test DBEngine CLI method error handling."""
    mock_dbms = Mock(spec=DBMS)
    mock_parser = Mock()
    mock_tokenizer = Mock()

    # Mock the tokenizer and parser
    with patch('dumbdb.db_engine.Tokenizer') as mock_tokenizer_class, \
            patch('builtins.input') as mock_input, \
            patch('builtins.print') as mock_print, \
            patch('traceback.print_exc') as mock_traceback:

        mock_tokenizer_class.return_value = mock_tokenizer
        mock_tokenizer.tokenize.return_value = ['INVALID', 'QUERY', ';']
        mock_parser.parse.return_value = None

        # Setup mock input to return an invalid query and then 'exit'
        mock_input.side_effect = ["INVALID QUERY;", "exit"]

        # Create engine with mocked components
        engine = DBEngine(dbms=mock_dbms, parser=mock_parser)

        # Run CLI
        engine.cli()

        # Verify results
        assert mock_input.call_count == 2
        assert mock_tokenizer.tokenize.call_count == 1
        assert mock_parser.parse.call_count == 1
        mock_print.assert_called()
        mock_traceback.assert_called_once()
