from dataclasses import dataclass, field
from typing import List, Optional


class ASTNode:
    """Base class for all AST nodes."""
    pass


@dataclass
class Column(ASTNode):
    name: str

    def __str__(self):
        if not isinstance(self.name, str) and self.name != "*":
            raise ValueError(
                f"Column name must be a string or *, got {self.name}")
        return self.name


@dataclass
class Table(ASTNode):
    name: str


@dataclass
class WhereCondition(ASTNode):
    """Base class for all WHERE conditions."""
    pass


@dataclass
class EqualsCondition(WhereCondition):
    column: Column
    value: str


@dataclass
class AndCondition(WhereCondition):
    left: WhereCondition
    right: WhereCondition


@dataclass
class Query(ASTNode):
    """Base class for all query nodes."""
    pass


@dataclass
class CreateDatabaseQuery(Query):
    database: str


@dataclass
class ShowDatabasesQuery(Query):
    pass


@dataclass
class DropDatabaseQuery(Query):
    database: str


@dataclass
class UseDatabaseQuery(Query):
    database: str


@dataclass
class CreateTableQuery(Query):
    table: Table
    columns: List[Column]


@dataclass
class ShowTablesQuery(Query):
    pass


@dataclass
class DropTableQuery(Query):
    table: Table


@dataclass
class SelectQuery(Query):
    columns: List[Column]
    table: Table
    where_clause: Optional[WhereCondition] = None


@dataclass
class InsertQuery(Query):
    table: Table
    columns: List[Column]
    values: List[str]
    row: dict[Column, str] = field(init=False)

    def __post_init__(self):
        self.row = dict(zip(self.columns, self.values))


@dataclass
class UpdateQuery(Query):
    """Represents an UPDATE query in the AST.

    Example:
        UPDATE users SET name = 'John' WHERE id = 1;
    """
    table: Table
    set_clause: dict[str, str]  # column -> value mapping
    where_clause: Optional[WhereCondition] = None


@dataclass
class DeleteQuery:
    """Represents a DELETE query in the AST.

    Example:
        DELETE FROM users WHERE id = 1;
    """
    table: Table
    where_clause: Optional[WhereCondition] = None
