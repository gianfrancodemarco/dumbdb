import csv
import time
from dataclasses import dataclass
from pathlib import Path

from .database import Database
from .models import QueryResult


@dataclass
class AppendOnlyDatabase(Database):
    """
    A class representing an append-only database.
    This database does not store data in memory, but rather on disk.
    It only appends data to the end of the file. For each primary key, the last record is the valid one.
    """
    name: str
    root_dir: Path = Path("./data")

    @property
    def tables_dir(self) -> Path:
        return self.root_dir / f"{self.name}/tables"

    def __init__(self, name: str, root_dir: Path):
        self.name = name
        self.root_dir = root_dir
        if not self.tables_dir.exists():
            self.tables_dir.mkdir(parents=True)

    def get_table_file_path(self, table_name: str) -> Path:
        return self.tables_dir / f"{table_name}.csv"

    def create_table(self, table_name: str, headers: list[str] = None) -> None:
        """Create a new table in the database - which is just a new csv file."""
        table_file = self.get_table_file_path(table_name)

        if table_file.exists():
            raise ValueError(f"Table '{table_name}' already exists")

        if not headers:
            headers = ["id"]

        headers.append("__deleted__")

        # Create an empty file for the table with headers
        with open(table_file, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(headers)

    def insert(self, table_name: str, value: dict):
        """Insert a new row into a table."""
        table_file = self.get_table_file_path(table_name)
        if not table_file.exists():
            raise ValueError(f"Table '{table_name}' does not exist")

        with open(table_file, 'a', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(list(value.values()) + [False])

    def update(self, table_name: str, value: dict) -> None:
        """
        Update a row in a table.
        For append-only databases, an update is just an insert.
        Before inserting, we check if the row already exists. If not, we throw an error.
        """
        table_file = self.get_table_file_path(table_name)
        if not table_file.exists():
            raise ValueError(f"Table '{table_name}' does not exist")

        query_result = self.query(table_name, {"id": value["id"]})
        if not query_result.rows:
            raise ValueError(f"Row with id {value['id']} does not exist")

        self.insert(table_name, value)

    def delete(self, table_name: str, value: dict) -> None:
        """
        Delete a row from a table.
        For append-only databases, a delete is an append with a special value to signal the deletion.
        """
        table_file = self.get_table_file_path(table_name)
        if not table_file.exists():
            raise ValueError(f"Table '{table_name}' does not exist")

        with open(table_file, 'a', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(list(value.values()) + [True])

    def query(self, table_name: str, query: dict) -> QueryResult:
        """Query data from a table."""
        start_time = time.time()
        table_file = self.get_table_file_path(table_name)
        if not table_file.exists():
            raise ValueError(f"Table '{table_name}' does not exist")

        matching_rows = {}
        with open(table_file, 'r', newline='') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                if all(row[key] == query[key] for key in query):
                    matching_rows[row['id']] = row

        # Remove all rows for which the last line is a delete
        matching_rows = {k: v for k,
                         v in matching_rows.items() if v['__deleted__'] == 'False'}

        return QueryResult(time.time() - start_time, list(matching_rows.values()))

    def pretty_query(self, table_name: str, query: dict) -> QueryResult:
        """Pretty print a query."""
        data = self.query(table_name, query)
        if not data.rows:
            print("No results found")
            return data

        headers = data.rows[0].keys()
        header_str = ",".join(headers)
        print("-" * len(header_str))
        print(header_str)
        print("-" * len(header_str))
        for row in data.rows:
            print(",".join(str(value) for value in row.values()))
        print("-" * len(header_str))
        print(f"Time: {data.time*1000:.4f} ms")
        return data

    def drop_table(self, table_name: str) -> None:
        """Drop a table.

        Args:
            table_name(str): The name of the table to drop.
        """
        table_file = self.get_table_file_path(table_name)
        if not table_file.exists():
            raise ValueError(f"Table '{table_name}' does not exist")

        table_file.unlink()

    def compact_table(self, table_name: str):
        """Compact a table.

        Args:
            table_name(str): The name of the table to compact.
        """
        table_file = self.get_table_file_path(table_name)
        if not table_file.exists():
            raise ValueError(f"Table '{table_name}' does not exist")

        compacted_data = {}
        with open(table_file, 'r', newline='') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                compacted_data[row['id']] = row

        # Remove all rows for which the last line is a delete
        compacted_data = {k: v for k,
                          v in compacted_data.items() if v['__deleted__'] == 'False'}

        with open(table_file, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            # Write headers from the first row's keys
            csv_writer.writerow(list(compacted_data.values())[0].keys())
            for row in compacted_data.values():
                csv_writer.writerow(row.values())
