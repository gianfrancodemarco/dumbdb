from dataclasses import dataclass, field


@dataclass
class Table:
    name: str
    data: list[dict] = field(default_factory=list)
    header: list[str] = field(default_factory=list)


@dataclass
class QueryResult:
    time: float
    rows: list[dict]
