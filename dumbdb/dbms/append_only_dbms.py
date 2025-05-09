import csv
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from dumbdb.dbms.dbms import (DBMS, QueryResult, require_exists_database,
                              require_exists_table, require_isset_database,
                              require_not_exists_table)
from dumbdb.parser.ast import (AndCondition, Column, EqualsCondition,
                               WhereCondition)


@dataclass
class AppendOnlyDBMS(DBMS):
    """
    A class representing an append-only DBMS.
    This DBMS does not store data in memory, but rather on disk.
    It only appends data to the end of the file. For each primary key, the last record is the valid one.
    """

    def get_database_dir(self, db_name: str) -> Path:
        return self.root_dir / db_name

    def get_tables_dir(self, db_name: str) -> Path:
        return self.get_database_dir(db_name) / "tables"

    @property
    def current_database_dir(self) -> Path:
        return self.get_database_dir(self.current_database)

    @property
    def tables_dir(self) -> Path:
        return self.get_tables_dir(self.current_database)

    def create_database(self, db_name: str) -> QueryResult:
        db_dir = self.get_database_dir(db_name)
        if db_dir.exists():
            raise ValueError(f"Database '{db_name}' already exists")

        tables_dir = self.get_tables_dir(db_name)
        tables_dir.mkdir(parents=True)

        return QueryResult()

    def show_databases(self) -> QueryResult:
        return QueryResult(rows=[f.stem for f in self.root_dir.iterdir() if f.is_dir()])

    def drop_database(self, db_name: str) -> QueryResult:
        db_dir = self.get_database_dir(db_name)
        if not db_dir.exists():
            raise ValueError(f"Database '{db_name}' does not exist")

        shutil.rmtree(db_dir)

        return QueryResult()

    @require_exists_database
    def use_database(self, db_name: str) -> QueryResult:
        self.current_database = db_name
        return QueryResult()

    def get_table_file_path(self, table_name: str) -> Path:
        return self.tables_dir / f"{table_name}.csv"

    @require_isset_database
    def show_tables(self) -> QueryResult:
        return QueryResult(rows=[f.stem for f in self.get_tables_dir(self.current_database).iterdir() if f.is_file()])

    @require_isset_database
    @require_not_exists_table
    def create_table(self, table_name: str, headers: list[str] = None) -> QueryResult:
        """Create a new table in the database - which is just a new csv file."""
        table_file = self.get_table_file_path(table_name)

        if not headers:
            headers = ["id"]

        headers.append("__deleted__")

        # Create an empty file for the table with headers
        with open(table_file, 'w', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(headers)

        return QueryResult()

    @require_isset_database
    @require_exists_table
    def insert(self, table_name: str, row: dict) -> QueryResult:
        """Insert a new row into a table."""
        table_file = self.get_table_file_path(table_name)
        with open(table_file, 'a', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(list(row.values()) + [False])

        return QueryResult()

    @require_isset_database
    @require_exists_table
    def update(self, table_name: str, set_clause: dict, where_clause: WhereCondition = None) -> QueryResult:
        """
        Update rows in a table that match the where clause.
        For append-only databases, an update is just an insert.
        We first find all matching rows, then update each one.
        """
        # In append only databases, we cannot update the id field since it is the primary key.
        if "id" in set_clause:
            raise ValueError(
                "Cannot update the id field in append-only databases")

        # Find all rows that match the where clause
        matching_rows = self.query(table_name, where_clause).rows

        if not matching_rows:
            return QueryResult()  # No rows to update

        # Update each matching row
        for row in matching_rows:
            # Create new row with updated values
            updated_row = row.copy()
            updated_row.update(set_clause)
            self.insert(table_name, updated_row)

        return QueryResult()

    @require_isset_database
    @require_exists_table
    def delete(self, table_name: str, where_clause: WhereCondition = None) -> QueryResult:
        """
        Delete rows from a table that match the where clause.
        For append-only databases, a delete is an append with a special value to signal the deletion.
        """
        # Find all rows that match the where clause
        matching_rows = self.query(table_name, where_clause).rows

        if not matching_rows:
            return QueryResult()  # No rows to delete

        # Delete each matching row
        with open(self.get_table_file_path(table_name), 'a', newline='') as f:
            csv_writer = csv.writer(f)
            for row in matching_rows:
                csv_writer.writerow(list(row.values()) + [True])

        return QueryResult()

    @require_isset_database
    @require_exists_table
    def query(self, table_name: str, where_clause=None) -> QueryResult:
        """Query data from a table."""
        start_time = time.time()
        table_file = self.get_table_file_path(table_name)

        matching_rows = {}
        with open(table_file, 'r', newline='') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                matching_rows[row['id']] = row

        # Remove all rows for which the last line is a delete
        matching_rows = {k: v for k,
                         v in matching_rows.items() if v['__deleted__'] == 'False'}

        # Apply WHERE clause if present
        if where_clause is not None:
            matching_rows = {k: v for k, v in matching_rows.items()
                             if self.evaluate_where_clause(v, where_clause)}

        # Remove the __deleted__ column
        for row in matching_rows.values():
            del row['__deleted__']

        return QueryResult(

            time=time.time() - start_time,
            rows=list(matching_rows.values())
        )

    def evaluate_where_clause(self, row: dict, where_clause) -> bool:
        """Evaluate a WHERE clause against a row."""
        if isinstance(where_clause, EqualsCondition):
            return str(row[where_clause.column.name]) == where_clause.value.strip("'")
        elif isinstance(where_clause, AndCondition):
            return (self.evaluate_where_clause(row, where_clause.left) and
                    self.evaluate_where_clause(row, where_clause.right))
        return False

    @require_isset_database
    @require_exists_table
    def drop_table(self, table_name: str) -> QueryResult:
        """Drop a table."""
        table_file = self.get_table_file_path(table_name)
        table_file.unlink()
        return QueryResult()

    @require_isset_database
    @require_exists_table
    def compact_table(self, table_name: str) -> QueryResult:
        """Compact a table."""
        table_file = self.get_table_file_path(table_name)

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

        return QueryResult()

    @require_isset_database
    @require_exists_table
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
