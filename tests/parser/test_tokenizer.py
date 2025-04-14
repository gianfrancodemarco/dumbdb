import pytest
from dumbdb.parser.tokenizer import Tokenizer, TokenType


def test_basic_select_query():
    tokenizer = Tokenizer()
    sql = "SELECT * FROM users;"
    tokens = tokenizer.tokenize(sql)
    expected = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.STAR, "*"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    assert tokens == expected


def test_select_with_columns():
    tokenizer = Tokenizer()
    sql = "SELECT id, name, age FROM users;"
    tokens = tokenizer.tokenize(sql)
    expected = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.IDENTIFIER, "id"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "name"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "age"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    assert tokens == expected


def test_insert_query():
    tokenizer = Tokenizer()
    sql = "INSERT INTO users VALUES (1, 'John', 25);"
    tokens = tokenizer.tokenize(sql)
    expected = [
        (TokenType.INSERT, "INSERT"),
        (TokenType.INTO, "INTO"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.VALUES, "VALUES"),
        (TokenType.LPAREN, "("),
        (TokenType.LITERAL, "1"),
        (TokenType.COMMA, ","),
        (TokenType.LITERAL, "'John'"),
        (TokenType.COMMA, ","),
        (TokenType.LITERAL, "25"),
        (TokenType.RPAREN, ")"),
        (TokenType.SEMICOLON, ";")
    ]
    assert tokens == expected


def test_string_literals():
    tokenizer = Tokenizer()
    sql = "SELECT 'hello', \"world\" FROM test;"
    tokens = tokenizer.tokenize(sql)
    expected = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.LITERAL, "'hello'"),
        (TokenType.COMMA, ","),
        (TokenType.LITERAL, "\"world\""),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "test"),
        (TokenType.SEMICOLON, ";")
    ]
    assert tokens == expected


def test_numeric_literals():
    tokenizer = Tokenizer()
    sql = "SELECT 42, 3.14, -1.5 FROM numbers;"
    tokens = tokenizer.tokenize(sql)
    expected = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.LITERAL, "42"),
        (TokenType.COMMA, ","),
        (TokenType.LITERAL, "3.14"),
        (TokenType.COMMA, ","),
        (TokenType.LITERAL, "-1.5"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "numbers"),
        (TokenType.SEMICOLON, ";")
    ]
    assert tokens == expected


def test_case_insensitive_keywords():
    tokenizer = Tokenizer()
    sql = "select * from users;"
    tokens = tokenizer.tokenize(sql)
    expected = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.STAR, "*"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    assert tokens == expected


def test_whitespace_handling():
    tokenizer = Tokenizer()
    sql = "SELECT  \t\n  *  \n  FROM  \t  users  ;"
    tokens = tokenizer.tokenize(sql)
    expected = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.STAR, "*"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "users"),
        (TokenType.SEMICOLON, ";")
    ]
    assert tokens == expected


def test_complex_identifiers():
    tokenizer = Tokenizer()
    sql = "SELECT user_id, first_name, last_name FROM user_profiles;"
    tokens = tokenizer.tokenize(sql)
    expected = [
        (TokenType.SELECT, "SELECT"),
        (TokenType.IDENTIFIER, "user_id"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "first_name"),
        (TokenType.COMMA, ","),
        (TokenType.IDENTIFIER, "last_name"),
        (TokenType.FROM, "FROM"),
        (TokenType.IDENTIFIER, "user_profiles"),
        (TokenType.SEMICOLON, ";")
    ]
    assert tokens == expected


def test_empty_string():
    tokenizer = Tokenizer()
    sql = ""
    tokens = tokenizer.tokenize(sql)
    assert tokens == []


def test_invalid_character():
    tokenizer = Tokenizer()
    sql = "SELECT @ FROM users;"
    with pytest.raises(Exception) as exc_info:
        tokenizer.tokenize(sql)
    assert "Illegal character" in str(exc_info.value)


# def test_expressions():
#     """
#     The tokernizer cannot handle expressions yet, so this should fail.
#     """

#     tokenizer = Tokenizer()
#     sql = "SELECT (1 + (2 * 3)) FROM calculations;"
#     tokens = tokenizer.tokenize(sql)
#     expected = [
#         (TokenType.SELECT, "SELECT"),
#         (TokenType.LPAREN, "("),
#         (TokenType.LITERAL, "1"),
#         (TokenType.IDENTIFIER, "+"),
#         (TokenType.LPAREN, "("),
#         (TokenType.LITERAL, "2"),
#         (TokenType.IDENTIFIER, "*"),
#         (TokenType.LITERAL, "3"),
#         (TokenType.RPAREN, ")"),
#         (TokenType.RPAREN, ")"),
#         (TokenType.FROM, "FROM"),
#         (TokenType.IDENTIFIER, "calculations"),
#         (TokenType.SEMICOLON, ";")
#     ]
#     assert tokens == expected
