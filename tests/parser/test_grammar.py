import pytest
from dumbdb.parser.grammar import (
    GrammarRule, Literal, OrRule, MultipleRule,
    LiteralRule, Or, Multiple, ParseResult
)
from dumbdb.parser.tokenizer import TokenType


def test_literal_rule_success():
    """Test Literal rule matching a specific token type."""
    rule = LiteralRule(TokenType.SELECT)
    tokens = [(TokenType.SELECT, "SELECT"), (TokenType.FROM, "FROM")]
    result = rule.parse(tokens, 0)
    assert result is not None
    value, pos = result
    assert value == "SELECT"
    assert pos == 1


def test_literal_rule_failure():
    """Test Literal rule failing to match."""
    rule = LiteralRule(TokenType.SELECT)
    tokens = [(TokenType.FROM, "FROM"), (TokenType.SELECT, "SELECT")]
    result = rule.parse(tokens, 0)
    assert result is None


def test_or_rule_success():
    """Test Or rule matching one of multiple alternatives."""
    rule = Or(
        LiteralRule(TokenType.SELECT),
        LiteralRule(TokenType.INSERT)
    )
    tokens = [(TokenType.INSERT, "INSERT"), (TokenType.INTO, "INTO")]
    result = rule.parse(tokens, 0)
    assert result is not None
    value, pos = result
    assert value == "INSERT"
    assert pos == 1


def test_or_rule_failure():
    """Test Or rule failing to match any alternative."""
    rule = Or(
        LiteralRule(TokenType.SELECT),
        LiteralRule(TokenType.INSERT)
    )
    tokens = [(TokenType.FROM, "FROM"), (TokenType.INTO, "INTO")]
    result = rule.parse(tokens, 0)
    assert result is None


def test_multiple_rule_success():
    """Test Multiple rule matching zero or more occurrences."""
    rule = Multiple(LiteralRule(TokenType.IDENTIFIER))
    tokens = [
        (TokenType.IDENTIFIER, "id"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.FROM, "FROM")
    ]
    result = rule.parse(tokens, 0)
    assert result is not None
    values, pos = result
    assert values == ["id", "name"]
    assert pos == 3


def test_multiple_rule_empty():
    """Test Multiple rule matching zero occurrences."""
    rule = Multiple(LiteralRule(TokenType.IDENTIFIER))
    tokens = [(TokenType.FROM, "FROM")]
    # Fail if no occurrence found.
    result = rule.parse(tokens, 0)
    assert result is None


def test_multiple_rule_with_commas():
    """Test Multiple rule handling commas between items."""
    rule = Multiple(LiteralRule(TokenType.IDENTIFIER))
    tokens = [
        (TokenType.IDENTIFIER, "id"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "age"),
        (TokenType.FROM, "FROM")
    ]
    result = rule.parse(tokens, 0)
    assert result is not None
    values, pos = result
    assert values == ["id", "name", "age"]
    assert pos == 5


def test_complex_combination():
    """Test combination of different rule types."""
    # Example: SELECT id, name FROM table
    select_clause = Or(
        LiteralRule(TokenType.SELECT),
        LiteralRule(TokenType.INSERT)
    )
    column_list = Multiple(LiteralRule(TokenType.IDENTIFIER))
    from_clause = Or(
        LiteralRule(TokenType.FROM),
        LiteralRule(TokenType.INTO)
    )

    tokens = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users")
    ]

    # Test SELECT
    result = select_clause.parse(tokens, 0)
    assert result is not None
    value, pos = result
    assert value == "SELECT"
    assert pos == 1

    # Test column list
    result = column_list.parse(tokens, pos)
    assert result is not None
    values, pos = result
    assert values == ["id", "name"]
    assert pos == 4

    # Test FROM
    result = from_clause.parse(tokens, pos)
    assert result is not None
    value, pos = result
    assert value == "FROM"
    assert pos == 5


def test_parse_result_type():
    """Test that ParseResult is properly typed."""
    result: ParseResult = ("value", 1)
    value, pos = result
    assert isinstance(value, str)
    assert isinstance(pos, int)


def test_grammar_rule_abstract():
    """Test that GrammarRule is abstract and cannot be instantiated."""
    with pytest.raises(NotImplementedError):
        GrammarRule().parse([], 0)
