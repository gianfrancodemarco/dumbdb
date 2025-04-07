from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

from dumbdb.models import QueryResult


def require_isset_database(func):
    """
    Decorator that checks if a database is selected before executing a method.
    Raises a ValueError if no database is selected.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.current_database is None:
            raise ValueError(
                "No database selected. Use 'use_database' to select a database first.")
        return func(self, *args, **kwargs)
    return wrapper


@dataclass
class DBMS(ABC):
    root_dir: Path = Path("./data")
    current_database: str | None = None

    @abstractmethod
    def create_database(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def use_database(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def create_table(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def drop_table(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def insert(table: str, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update(table: str, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete(table: str, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def query(table: str, query: dict) -> QueryResult:
        raise NotImplementedError()
