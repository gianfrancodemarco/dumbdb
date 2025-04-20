import pytest

from dumbdb.parser.ast import (AndCondition, Column, EqualsCondition,
                               SelectQuery, Table, WhereCondition)
from dumbdb.parser.parser import Parser
from dumbdb.parser.tokenizer import Tokenizer


def test_simple_where_condition():
    query = "SELECT * FROM users WHERE id = 1"
    tokens = Tokenizer().tokenize(query)
    ast = Parser().parse(tokens)

    assert isinstance(ast, SelectQuery)
    assert ast.table.name == "users"
    assert isinstance(ast.where_clause, EqualsCondition)
    assert ast.where_clause.column.name == "id"
    assert ast.where_clause.value == "1"


def test_and_where_condition():
    query = "SELECT * FROM users WHERE id = 1 AND name = 'John'"
    tokens = Tokenizer().tokenize(query)
    ast = Parser().parse(tokens)

    assert isinstance(ast, SelectQuery)
    assert ast.table.name == "users"
    assert isinstance(ast.where_clause, AndCondition)

    left_condition = ast.where_clause.left
    right_condition = ast.where_clause.right

    assert isinstance(left_condition, EqualsCondition)
    assert left_condition.column.name == "id"
    assert left_condition.value == "1"

    assert isinstance(right_condition, EqualsCondition)
    assert right_condition.column.name == "name"
    assert right_condition.value == "'John'"


def test_multiple_where_conditions():
    query = "SELECT * FROM users WHERE id = 1 AND name = 'John' AND age = 20"
    tokens = Tokenizer().tokenize(query)
    ast = Parser().parse(tokens)

    assert isinstance(ast, SelectQuery)
    assert ast.table.name == "users"
    assert isinstance(ast.where_clause, AndCondition)

    left_condition = ast.where_clause.left
    right_condition = ast.where_clause.right

    assert isinstance(left_condition, EqualsCondition)
    assert left_condition.column.name == "id"
    assert left_condition.value == "1"

    assert isinstance(right_condition, AndCondition)
    left_condition = right_condition.left
    right_condition = right_condition.right

    assert isinstance(left_condition, EqualsCondition)
    assert left_condition.column.name == "name"
    assert left_condition.value == "'John'"

    assert isinstance(right_condition, EqualsCondition)
    assert right_condition.column.name == "age"
    assert right_condition.value == "20"


def test_even_more_complex_where_and_conditions():
    query = "SELECT * FROM users WHERE id = 1 AND name = 'John' AND age = 20 AND email = 'john@example.com' AND is_active = 1"
    tokens = Tokenizer().tokenize(query)
    ast = Parser().parse(tokens)

    assert isinstance(ast, SelectQuery)
    assert ast.table.name == "users"
    assert isinstance(ast.where_clause, AndCondition)

    left_condition = ast.where_clause.left
    right_condition = ast.where_clause.right

    assert isinstance(left_condition, EqualsCondition)
    assert left_condition.column.name == "id"
    assert left_condition.value == "1"

    assert isinstance(right_condition, AndCondition)
    left_condition = right_condition.left
    right_condition = right_condition.right

    assert isinstance(left_condition, EqualsCondition)
    assert left_condition.column.name == "name"
    assert left_condition.value == "'John'"

    assert isinstance(right_condition, AndCondition)
    left_condition = right_condition.left
    right_condition = right_condition.right

    assert isinstance(left_condition, EqualsCondition)
    assert left_condition.column.name == "age"
    assert left_condition.value == "20"

    assert isinstance(right_condition, AndCondition)
    left_condition = right_condition.left
    right_condition = right_condition.right

    assert isinstance(left_condition, EqualsCondition)
    assert left_condition.column.name == "email"
    assert left_condition.value == "'john@example.com'"

    assert isinstance(right_condition, EqualsCondition)
    assert right_condition.column.name == "is_active"
    assert right_condition.value == "1"


def test_invalid_where_condition():
    # Test invalid syntax
    query = "SELECT * FROM users WHERE id ="
    tokens = Tokenizer().tokenize(query)
    with pytest.raises(Exception):
        Parser().parse(tokens)

    # Test invalid operator
    query = "SELECT * FROM users WHERE id  1"
    tokens = Tokenizer().tokenize(query)
    with pytest.raises(Exception):
        Parser().parse(tokens)
