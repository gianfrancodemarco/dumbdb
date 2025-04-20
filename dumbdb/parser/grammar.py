from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from dumbdb.parser.ast import AndCondition, Column, EqualsCondition
from dumbdb.parser.tokenizer import TokenType

# A ParseResult holds the parsed value and the new token position.
ParseResult = Tuple[Any, int]


class GrammarRule:
    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        raise NotImplementedError("Must implement in subclass")

# A literal rule expects a specific token type.


@dataclass
class Literal(GrammarRule):
    token_type: TokenType

    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        if pos < len(tokens):
            token = tokens[pos]
            if token[0] == self.token_type:
                # Return the token value.
                return token[1], pos + 1
        return None  # Fail

    def __repr__(self):
        return f"Literal({self.token_type})"


@dataclass
class OrRule(GrammarRule):
    """
    A rule that tries each rule in order until one succeeds.
    If all rules fail, the rule fails and returns None.
    """

    rules: List[GrammarRule]

    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        for rule in self.rules:
            result = rule.parse(tokens, pos)
            if result is not None:
                return result
        return None

    def __repr__(self):
        return f"Or({self.rules})"


@dataclass
class MultipleRule(GrammarRule):
    """
    A rule that matches one or more occurrences of a rule.
    """
    rule: GrammarRule

    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        results: List[Any] = []
        current = pos
        while True:
            result = self.rule.parse(tokens, current)
            if result is None:
                break
            value, next_pos = result
            results.append(value)
            current = next_pos

            # After a rule, we might have a comma
            result = LiteralRule(
                TokenType.COMMA).parse(tokens, current)
            if result is not None:
                current = result[1]

        # Fail if no occurrence found.
        if not results:
            return None
        return results, current

    def __repr__(self):
        return f"Multiple({self.rule})"

# Convenience helper functions.


def LiteralRule(token_type: str) -> Literal:
    return Literal(token_type)


def Or(*rules: Any) -> OrRule:
    # Allow rules to be either plain strings or GrammarRule instances.
    rule_objs = [LiteralRule(r) if isinstance(r, str) else r for r in rules]
    return OrRule(rule_objs)


def Multiple(rule: Any) -> MultipleRule:
    if isinstance(rule, str):
        rule = LiteralRule(rule)
    return MultipleRule(rule)


class WhereClauseRule(GrammarRule):
    """
    A where clause is either:
    - a simple condition in the form of <column_name> <operator> <value>
    - an AND condition in the form of <condition> AND <condition> [AND <condition> ...]
    """

    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        # Check for WHERE keyword
        result = LiteralRule(TokenType.WHERE).parse(tokens, pos)
        if result is None:
            return None

        # Parse the condition
        where_keyword, new_pos = result
        # NOTE: We first try to parse an AND condition, if that fails, we try to parse a simple condition.
        # This is because the AND condition is more specific than the simple condition.
        # If we try to parse a simple condition first, if would work even if the condition is an AND condition.
        result = Or(
            AndConditionRule(),
            SimpleConditionRule()
        ).parse(tokens, new_pos)
        if result is None:
            return None

        where_clause, final_pos = result
        return where_clause, final_pos


class AndConditionRule(GrammarRule):
    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        # Parse left condition
        left_result = SimpleConditionRule().parse(tokens, pos)
        if left_result is None:
            return None

        left_condition, new_pos = left_result

        # Check for AND keyword
        result = LiteralRule(TokenType.AND).parse(tokens, new_pos)
        if result is None:
            return None

        # NOTE:
        # Right can be a simple condition or an another AND condition.
        # Basically, right is another WhereClauseRule without the WHERE keyword.
        right_result = Or(
            AndConditionRule(),
            SimpleConditionRule()
        ).parse(tokens, result[1])
        if right_result is None:
            return None

        right_condition, final_pos = right_result

        return AndCondition(left_condition, right_condition), final_pos


class SimpleConditionRule(GrammarRule):
    """
    A simple condition is an expression in the form of <column_name> <operator> <value>
    """

    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        # Parse column name
        column_result = LiteralRule(TokenType.IDENTIFIER).parse(tokens, pos)
        if column_result is None:
            return None

        column_name, new_pos = column_result

        # Check for equals operator
        result = LiteralRule(TokenType.EQUALS).parse(tokens, new_pos)
        if result is None:
            return None

        # Parse value
        value_result = LiteralRule(TokenType.LITERAL).parse(tokens, result[1])
        value, final_pos = value_result

        return EqualsCondition(Column(column_name), value), final_pos
