from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import wraps
from pathlib import Path


@dataclass
class QueryResult:
    time: float
    rows: list[dict]


def extract_param_from_args_or_kwargs(param_name: str, args: list, kwargs: dict):
    """
    Extract a parameter from args or kwargs of a function.
    """

    if args and len(args) > 0:
        return args[0]
    elif kwargs and param_name in kwargs:
        return kwargs[param_name]
    else:
        raise ValueError(
            f"Parameter '{param_name}' not found in args or kwargs")


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


def require_exists_database(func):
    """
    Decorator that checks if a database exists before executing a method.
    Raises a ValueError if the database does not exist.
    Works by getting a db_name parameter from args or kwargs of the decorated function.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        db_name = extract_param_from_args_or_kwargs("db_name", args, kwargs)

        if db_name not in self.show_databases():
            raise ValueError(f"Database '{db_name}' does not exist")
        return func(self, *args, **kwargs)
    return wrapper


def require_exists_table(func):
    """
    Decorator that checks if a table exists before executing a method.
    Raises a ValueError if the table does not exist.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        table_name = extract_param_from_args_or_kwargs(
            "table_name", args, kwargs)
        if table_name not in self.show_tables():
            raise ValueError(f"Table '{table_name}' does not exist")
        return func(self, *args, **kwargs)
    return wrapper


def require_not_exists_table(func):
    """
    Decorator that checks if a table does not exist before executing a method.
    Raises a ValueError if the table exists.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        table_name = extract_param_from_args_or_kwargs(
            "table_name", args, kwargs)
        if table_name in self.show_tables():
            raise ValueError(f"Table '{table_name}' already exists")
        return func(self, *args, **kwargs)
    return wrapper


@dataclass
class DBMS(ABC):
    root_dir: Path = Path("./data")
    current_database: str | None = None

    @abstractmethod
    def create_database(db_name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def show_databases(self) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def drop_database(db_name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def use_database(db_name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def show_tables(self) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def create_table(table_name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def show_tables(self) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def drop_table(table_name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def insert(table_name: str, row: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update(table_name: str, row: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete(table_name: str, row: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def query(table_name: str, query: dict) -> QueryResult:
        raise NotImplementedError()
