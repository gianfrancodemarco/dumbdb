from dataclasses import dataclass
from typing import Any, ClassVar, List, Optional, Tuple

from dumbdb.parser.ast import Column, InsertQuery, Query, SelectQuery, Table
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


class Parser:
    def __init__(self):
        self.parsers = {
            TokenType.SELECT: SelectQueryParser(),
            TokenType.INSERT: InsertQueryParser(),
        }

    def parse(self, tokens: List[Tuple[str, str]]) -> Optional[Query]:
        selector = tokens[0][0]
        if selector not in self.parsers:
            raise Exception(
                f"Invalid syntax; Unexpected token: {tokens[0][1]} at position {0}.")

        return self.parsers[selector].parse(tokens)
