from typing import Any, List, Optional, Tuple

from dumbdb.parser.tokenizer import TokenType

# A ParseResult holds the parsed value and the new token position.
ParseResult = Tuple[Any, int]


class GrammarRule:
    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        raise NotImplementedError("Must implement in subclass")

# A literal rule expects a specific token type.


class Literal(GrammarRule):
    def __init__(self, token_type: TokenType):
        self.token_type = token_type

    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        if pos < len(tokens):
            token = tokens[pos]
            if token[0] == self.token_type:
                # Return the token value.
                return token[1], pos + 1
        return None  # Fail

    def __repr__(self):
        return f"Literal({self.token_type})"

# A combinator for alternatives: try each rule in order.


class OrRule(GrammarRule):
    def __init__(self, rules: List[GrammarRule]):
        self.rules = rules

    def parse(self, tokens: List[Tuple[str, str]], pos: int) -> Optional[ParseResult]:
        for rule in self.rules:
            result = rule.parse(tokens, pos)
            if result is not None:
                return result
        return None  # All alternatives failed

    def __repr__(self):
        return f"Or({self.rules})"

# A combinator for multiple occurrences: parse 0 or more times.


class MultipleRule(GrammarRule):
    def __init__(self, rule: GrammarRule):
        self.rule = rule

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
