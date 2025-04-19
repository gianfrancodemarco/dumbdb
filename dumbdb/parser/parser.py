from dataclasses import dataclass
from typing import Any, ClassVar, Dict, List, Optional, Tuple

from dumbdb.parser.ast import (Column, CreateDatabaseQuery, CreateTableQuery,
                               InsertQuery, Query, SelectQuery, Table,
                               UseDatabaseQuery)
from dumbdb.parser.grammar import LiteralRule, Multiple, Or, ParseResult
from dumbdb.parser.tokenizer import TokenType


@dataclass
class BaseParser:
    grammar: ClassVar[List[Any]]

    def parse(self, tokens: List[Tuple[str, str]], pos: int = 0) -> Optional[ParseResult]:
        values = []
        current = pos
        for rule in self.grammar:
            result = rule.parse(tokens, current)
            if result is None:
                raise Exception(
                    f"Invalid syntax; Unexpected token: {tokens[current][1]} at position {current}.")
            value, current = result
            values.append(value)

        return self.build_ast(values)

    def build_ast(self, values: List[Any]) -> Query:
        raise NotImplementedError("Must implement in subclass")


@dataclass
class CreateDatabaseQueryParser(BaseParser):
    """
    Grammar:
    CREATE DATABASE <database_name>;
    """

    grammar = [
        LiteralRule(TokenType.CREATE),
        LiteralRule(TokenType.DATABASE),
        LiteralRule(TokenType.IDENTIFIER),
        LiteralRule(TokenType.SEMICOLON),
    ]

    def build_ast(self, values: List[Any]) -> CreateDatabaseQuery:
        return CreateDatabaseQuery(database=values[2])


@dataclass
class UseDatabaseQueryParser(BaseParser):
    """
    Grammar:
    USE <database_name>;
    """

    grammar = [
        LiteralRule(TokenType.USE),
        LiteralRule(TokenType.IDENTIFIER),
        LiteralRule(TokenType.SEMICOLON),
    ]

    def build_ast(self, values: List[Any]) -> UseDatabaseQuery:
        return UseDatabaseQuery(database=values[1])


@dataclass
class CreateTableQueryParser(BaseParser):
    """
    Grammar:
    CREATE TABLE <table_name> (<column_name>+);
    """

    grammar = [
        LiteralRule(TokenType.CREATE),
        LiteralRule(TokenType.TABLE),
        LiteralRule(TokenType.IDENTIFIER),
        LiteralRule(TokenType.LPAREN),
        Multiple(LiteralRule(TokenType.IDENTIFIER)),
        LiteralRule(TokenType.RPAREN),
        LiteralRule(TokenType.SEMICOLON),
    ]

    def build_ast(self, values: List[Any]) -> CreateTableQuery:
        return CreateTableQuery(table=Table(values[2]), columns=values[4])


@dataclass
class SelectQueryParser(BaseParser):
    """
    Grammar:
    SELECT [* | <column_name>+] FROM <table_name>;
    """

    grammar = [
        LiteralRule(TokenType.SELECT),
        Or(LiteralRule(TokenType.STAR), Multiple(
            LiteralRule(TokenType.IDENTIFIER))),
        LiteralRule(TokenType.FROM),
        LiteralRule(TokenType.IDENTIFIER),
        LiteralRule(TokenType.SEMICOLON),
    ]

    def build_ast(self, values: List[Any]) -> SelectQuery:
        return SelectQuery(
            columns=[Column(values[1])],
            table=Table(values[3]),
            where_clause=None
        )


@dataclass
class InsertQueryParser(BaseParser):
    """
    Grammar:
    INSERT INTO <table_name> (<column_name>+) VALUES (<value>+);
    """

    grammar = [
        LiteralRule(TokenType.INSERT),
        LiteralRule(TokenType.INTO),
        LiteralRule(TokenType.IDENTIFIER),
        LiteralRule(TokenType.LPAREN),
        Multiple(LiteralRule(TokenType.IDENTIFIER)),
        LiteralRule(TokenType.RPAREN),
        LiteralRule(TokenType.VALUES),
        LiteralRule(TokenType.LPAREN),
        Or(Multiple(LiteralRule(TokenType.IDENTIFIER)), Multiple(
            LiteralRule(TokenType.LITERAL))),
        LiteralRule(TokenType.RPAREN),
        LiteralRule(TokenType.SEMICOLON),
    ]

    def build_ast(self, values: List[Any]) -> InsertQuery:
        return InsertQuery(
            table=Table(values[2]),
            columns=values[4],
            values=values[8]
        )


@dataclass
class Parser:
    parsers: ClassVar[Dict[TokenType, Any]] = {
        TokenType.USE: UseDatabaseQueryParser(),
        TokenType.CREATE: {
            TokenType.DATABASE: CreateDatabaseQueryParser(),
            TokenType.TABLE: CreateTableQueryParser()
        },
        TokenType.SELECT: SelectQueryParser(),
        TokenType.INSERT: InsertQueryParser(),
    }

    def select_parser(
        self,
        current_parsers: Dict[TokenType, Any],
        tokens: List[Tuple[str, str]],
        current_token_idx: int
    ) -> BaseParser:
        """
        Select the appropriate parser based on the token list.

        The current_parsers is a dictionary of token types to parsers. Holds the current valid parsers for the already parsed tokens.
        If the current token is not in the current_parsers, raise an exception.
        If the current token select a single parser, return that parser.
        If the current token select multiple parsers, call recursively with the new parsers and the next token.
        """

        if current_token_idx >= len(tokens):
            raise Exception(
                f"Invalid syntax; Unexpected end of input at position {current_token_idx}. Expected one of {current_parsers.keys()}.")

        selector = tokens[current_token_idx][0]

        if selector not in current_parsers:
            raise Exception(
                f"Invalid syntax; Unexpected token: {tokens[current_token_idx][1]} at position {current_token_idx}.")

        selected_parsers = current_parsers[selector]
        if isinstance(selected_parsers, dict):
            return self.select_parser(selected_parsers, tokens, current_token_idx + 1)

        selected_parser = selected_parsers
        return selected_parser

    def parse(self, tokens: List[Tuple[str, str]]) -> Optional[Query]:
        parser = self.select_parser(self.parsers, tokens, 0)
        return parser.parse(tokens)
