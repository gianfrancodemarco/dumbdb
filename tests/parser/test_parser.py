import pytest

from dumbdb.parser.ast import (Column, CreateDatabaseQuery, CreateTableQuery,
                               InsertQuery, SelectQuery, Table,
                               UseDatabaseQuery, UpdateQuery, EqualsCondition,
                               DeleteQuery)
from dumbdb.parser.parser import (BaseParser, CreateDatabaseQueryParser,
                                  CreateTableQueryParser, InsertQueryParser,
                                  Parser, SelectQueryParser,
                                  UseDatabaseQueryParser, UpdateQueryParser,
                                  DeleteQueryParser)
from dumbdb.parser.tokenizer import TokenType


def test_base_parser_abstract():
    """Test that BaseParser is abstract and cannot be instantiated."""
    with pytest.raises(NotImplementedError):
        BaseParser().build_ast([])


def test_create_database_query_parser():
    """Test parsing a CREATE DATABASE query."""
    parser = CreateDatabaseQueryParser()
    tokens = [
        (TokenType.CREATE, "CREATE"),
        (TokenType.DATABASE, "DATABASE"),
        (TokenType.IDENTIFIER, "my_database"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, CreateDatabaseQuery)
    assert query.database == "my_database"


def test_use_database_query_parser():
    """Test parsing a USE DATABASE query."""
    parser = UseDatabaseQueryParser()
    tokens = [
        (TokenType.USE, "USE"),
        (TokenType.IDENTIFIER, "my_database"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, UseDatabaseQuery)
    assert query.database == "my_database"


def test_create_table_query_parser():
    """Test parsing a CREATE TABLE query."""
    parser = CreateTableQueryParser()
    tokens = [
        (TokenType.CREATE, "CREATE"),
        (TokenType.TABLE, "TABLE"),
        (TokenType.IDENTIFIER, "my_table"),
        (TokenType.LPAREN, "("),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.RPAREN, ")"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, CreateTableQuery)
    assert query.table == Table("my_table")
    assert query.columns == ["id", "name"]


def test_select_query_parser_simple():
    """Test parsing a simple SELECT query with all columns."""
    parser = SelectQueryParser()
    tokens = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.STAR, "*"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, SelectQuery)
    assert query.columns == [Column("*")]
    assert query.table == Table("users")
    assert query.where_clause is None


def test_select_query_parser_specific_columns():
    """Test parsing a SELECT query with specific columns."""
    parser = SelectQueryParser()
    tokens = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, SelectQuery)
    assert query.columns == [Column(["id", "name"])]
    assert query.table == Table("users")
    assert query.where_clause is None


def test_select_query_parser_invalid_syntax():
    """Test handling of invalid SELECT query syntax."""
    parser = SelectQueryParser()
    tokens = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.FROM, "FROM"),  # Missing column list
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    with pytest.raises(Exception) as exc_info:
        parser.parse(tokens)
    assert "Invalid syntax" in str(exc_info.value)


def test_insert_query_parser_simple():
    """Test parsing a simple INSERT query."""
    parser = InsertQueryParser()
    tokens = [
        (TokenType.INSERT, "INSERT"),
        (TokenType.INTO, "INTO"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.LPAREN, "("),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.RPAREN, ")"),
        (TokenType.VALUES, "VALUES"),
        (TokenType.LPAREN, "("),
        (TokenType.LITERAL, "1"),
        (TokenType.COMMA, ","),
        (TokenType.LITERAL, "'John'"),
        (TokenType.RPAREN, ")"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, InsertQuery)
    assert query.table == Table("users")
    assert query.columns == ["id", "name"]
    assert query.values == ["1", "'John'"]


def test_insert_query_parser_with_identifiers():
    """Test parsing an INSERT query with identifier values."""
    parser = InsertQueryParser()
    tokens = [
        (TokenType.INSERT, "INSERT"),
        (TokenType.INTO, "INTO"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.LPAREN, "("),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.RPAREN, ")"),
        (TokenType.VALUES, "VALUES"),
        (TokenType.LPAREN, "("),
        (TokenType.IDENTIFIER, "next_id"),
        (TokenType.RPAREN, ")"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, InsertQuery)
    assert query.table == Table("users")
    assert query.columns == ["id"]
    assert query.values == ["next_id"]


def test_insert_query_parser_invalid_syntax():
    """Test handling of invalid INSERT query syntax."""
    parser = InsertQueryParser()
    tokens = [
        (TokenType.INSERT, "INSERT"),
        (TokenType.INTO, "INTO"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.VALUES, "VALUES"),  # Missing column list
        (TokenType.LPAREN, "("),
        (TokenType.LITERAL, "1"),
        (TokenType.RPAREN, ")"),
        (TokenType.SEMICOLON, ";")
    ]
    with pytest.raises(Exception) as exc_info:
        parser.parse(tokens)
    assert "Invalid syntax" in str(exc_info.value)


def test_parser_select_query():
    """Test the main Parser class with a SELECT query."""
    parser = Parser()
    tokens = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.STAR, "*"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, SelectQuery)
    assert query.columns == [Column("*")]
    assert query.table == Table("users")


def test_parser_insert_query():
    """Test the main Parser class with an INSERT query."""
    parser = Parser()
    tokens = [
        (TokenType.INSERT, "INSERT"),
        (TokenType.INTO, "INTO"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.LPAREN, "("),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.RPAREN, ")"),
        (TokenType.VALUES, "VALUES"),
        (TokenType.LPAREN, "("),
        (TokenType.LITERAL, "1"),
        (TokenType.RPAREN, ")"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, InsertQuery)
    assert query.table == Table("users")
    assert query.columns == ["id"]
    assert query.values == ["1"]


def test_parser_unknown_query_type():
    """Test handling of unknown query types."""
    parser = Parser()
    tokens = [
        (TokenType.FROM, "FROM"),  # Not a valid query start
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    with pytest.raises(Exception) as exc_info:
        parser.parse(tokens)
    assert "Invalid syntax" in str(exc_info.value)


def test_update_query_parser_simple():
    """Test parsing a simple UPDATE query without WHERE clause."""
    parser = UpdateQueryParser()
    tokens = [
        (TokenType.UPDATE, "UPDATE"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SET, "SET"),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "'John'"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, UpdateQuery)
    assert query.table == Table("users")
    assert query.set_clause == {"name": "'John'"}
    assert query.where_clause is None


def test_update_query_parser_multiple_columns():
    """Test parsing an UPDATE query with multiple SET clauses."""
    parser = UpdateQueryParser()
    tokens = [
        (TokenType.UPDATE, "UPDATE"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SET, "SET"),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "'John'"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "age"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "25"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, UpdateQuery)
    assert query.table == Table("users")
    assert query.set_clause == {"name": "'John'", "age": "25"}
    assert query.where_clause is None


def test_update_query_parser_with_where():
    """Test parsing an UPDATE query with WHERE clause."""
    parser = UpdateQueryParser()
    tokens = [
        (TokenType.UPDATE, "UPDATE"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SET, "SET"),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "'John'"),
        (TokenType.WHERE, "WHERE"),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "1"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, UpdateQuery)
    assert query.table == Table("users")
    assert query.set_clause == {"name": "'John'"}
    assert isinstance(query.where_clause, EqualsCondition)
    assert query.where_clause.column == Column("id")
    assert query.where_clause.value == "1"


def test_update_query_parser_invalid_syntax():
    """Test handling of invalid UPDATE query syntax."""
    parser = UpdateQueryParser()
    tokens = [
        (TokenType.UPDATE, "UPDATE"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SET, "SET"),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "'John'"),
        (TokenType.WHERE, "WHERE"),  # Missing condition
        (TokenType.SEMICOLON, ";")
    ]
    with pytest.raises(Exception) as exc_info:
        parser.parse(tokens)
    assert "Invalid syntax" in str(exc_info.value)


def test_parser_update_query():
    """Test the main Parser class with an UPDATE query."""
    parser = Parser()
    tokens = [
        (TokenType.UPDATE, "UPDATE"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SET, "SET"),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "'John'"),
        (TokenType.WHERE, "WHERE"),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "1"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, UpdateQuery)
    assert query.table == Table("users")
    assert query.set_clause == {"name": "'John'"}
    assert isinstance(query.where_clause, EqualsCondition)
    assert query.where_clause.column == Column("id")
    assert query.where_clause.value == "1"


def test_delete_query_parser_simple():
    """Test parsing a simple DELETE query without WHERE clause."""
    parser = DeleteQueryParser()
    tokens = [
        (TokenType.DELETE, "DELETE"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, DeleteQuery)
    assert query.table == Table("users")
    assert query.where_clause is None


def test_delete_query_parser_with_where():
    """Test parsing a DELETE query with WHERE clause."""
    parser = DeleteQueryParser()
    tokens = [
        (TokenType.DELETE, "DELETE"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.WHERE, "WHERE"),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "1"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, DeleteQuery)
    assert query.table == Table("users")
    assert isinstance(query.where_clause, EqualsCondition)
    assert query.where_clause.column == Column("id")
    assert query.where_clause.value == "1"


def test_delete_query_parser_invalid_syntax():
    """Test handling of invalid DELETE query syntax."""
    parser = DeleteQueryParser()
    tokens = [
        (TokenType.DELETE, "DELETE"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.WHERE, "WHERE"),  # Missing condition
        (TokenType.SEMICOLON, ";")
    ]
    with pytest.raises(Exception) as exc_info:
        parser.parse(tokens)
    assert "Invalid syntax" in str(exc_info.value)


def test_parser_delete_query():
    """Test the main Parser class with a DELETE query."""
    parser = Parser()
    tokens = [
        (TokenType.DELETE, "DELETE"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.WHERE, "WHERE"),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.EQUALS, "="),
        (TokenType.LITERAL, "1"),
        (TokenType.SEMICOLON, ";")
    ]
    query = parser.parse(tokens)
    assert isinstance(query, DeleteQuery)
    assert query.table == Table("users")
    assert isinstance(query.where_clause, EqualsCondition)
    assert query.where_clause.column == Column("id")
    assert query.where_clause.value == "1"
