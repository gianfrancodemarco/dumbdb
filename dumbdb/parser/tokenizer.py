import re
from enum import Enum


class TokenType(Enum):
    """Enumeration of all possible token types in SQL-like queries.

    This enum defines all the token types that the tokenizer can recognize:
    - Keywords: SELECT, FROM, INSERT, INTO, VALUES
    - Symbols: *, ,, (, ), ;
    - Identifiers: table and column names
    - Literals: strings and numbers
    - Whitespace: spaces, tabs, newlines (these are skipped in the output)
    """
    CREATE = "CREATE"
    USE = "USE"
    DATABASE = "DATABASE"
    TABLE = "TABLE"
    SELECT = "SELECT"
    FROM = "FROM"
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    STAR = "*"
    COMMA = ","
    LPAREN = "("
    RPAREN = ")"
    SEMICOLON = ";"
    IDENTIFIER = "IDENTIFIER"
    LITERAL = "LITERAL"
    WS = "WS"


class Tokenizer:
    """A SQL-like query tokenizer.

    This class tokenizes SQL-like queries into a sequence of tokens. It supports:
    - Basic SQL keywords: SELECT, FROM, INSERT, INTO, VALUES
    - Common SQL symbols: *, ,, (, ), ;
    - Identifiers (table/column names): alphanumeric strings that may include underscores
    - String literals: single or double-quoted strings
    - Numeric literals: integers and floating-point numbers
    - Whitespace: spaces, tabs, newlines (these are skipped in the output)

    The tokenizer is case-insensitive for keywords but preserves case for identifiers.

    Example:
        >>> tokenizer = Tokenizer()
        >>> tokens = tokenizer.tokenize("SELECT * FROM users;")
        >>> print(tokens)
        [(TokenType.SELECT, "SELECT"), (TokenType.STAR, "*"), 
         (TokenType.FROM, "FROM"), (TokenType.IDENTIFIER, "users"), 
         (TokenType.SEMICOLON, ";")]
    """

    def __init__(self):
        """Initialize the tokenizer with predefined token patterns."""
        self.token_patterns = [
            (TokenType.CREATE,     r'CREATE\b'),
            (TokenType.USE,        r'USE\b'),
            (TokenType.DATABASE,   r'DATABASE\b'),
            (TokenType.TABLE,      r'TABLE\b'),
            (TokenType.SELECT,     r'SELECT\b'),
            (TokenType.FROM,       r'FROM\b'),
            (TokenType.INSERT,     r'INSERT\b'),
            (TokenType.INTO,       r'INTO\b'),
            (TokenType.VALUES,     r'VALUES\b'),
            (TokenType.STAR,       r'\*'),
            (TokenType.COMMA,      r','),
            (TokenType.LPAREN,     r'\('),
            (TokenType.RPAREN,     r'\)'),
            (TokenType.SEMICOLON,  r';'),
            (TokenType.IDENTIFIER, r'[A-Za-z_][A-Za-z0-9_]*'),
            (TokenType.LITERAL,    r'\'[^\']*\'|"[^\"]*"|-?\d+(\.\d+)?'),
            (TokenType.WS,         r'\s+'),
        ]

        # Pre-compile all regex patterns for better performance
        self.compiled_patterns = [
            (token_type, re.compile(pattern, re.IGNORECASE))
            for token_type, pattern in self.token_patterns
        ]

    def tokenize(self, sql: str) -> list[tuple[TokenType, str]]:
        """Tokenize a SQL-like query string into a sequence of tokens.

        Args:
            sql: The SQL-like query string to tokenize.

        Returns:
            A list of (TokenType, token_value) tuples representing the tokens.

        Raises:
            Exception: If an illegal character is encountered in the input string.

        Example:
            >>> tokenizer = Tokenizer()
            >>> tokens = tokenizer.tokenize("SELECT * FROM users;")
            >>> print(tokens)
            [(TokenType.SELECT, "SELECT"), (TokenType.STAR, "*"), 
             (TokenType.FROM, "FROM"), (TokenType.IDENTIFIER, "users"), 
             (TokenType.SEMICOLON, ";")]
        """
        tokens = []
        pos = 0
        while pos < len(sql):
            match = None
            for token_type, pattern in self.compiled_patterns:
                match = pattern.match(sql, pos)
                if match:
                    text = match.group(0)
                    if token_type != TokenType.WS:  # skip whitespace
                        # Normalize keywords to upper case
                        if token_type in (TokenType.SELECT, TokenType.FROM,
                                          TokenType.INSERT, TokenType.INTO,
                                          TokenType.VALUES):
                            tokens.append((token_type, text.upper()))
                        else:
                            tokens.append((token_type, text))
                    pos = match.end(0)
                    break
            if not match:
                raise Exception(f"Illegal character: {sql[pos]}")
        return tokens
