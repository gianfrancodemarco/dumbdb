import traceback
from dataclasses import dataclass, field

from dumbdb.dbms.append_only_dbms_with_hash_indexes import \
    AppendOnlyDBMSWithHashIndexes
from dumbdb.dbms.dbms import DBMS, QueryResult
from dumbdb.parser.ast import (CreateDatabaseQuery, CreateTableQuery,
                               DropDatabaseQuery, DropTableQuery, InsertQuery,
                               Query, SelectQuery, ShowDatabasesQuery,
                               ShowTablesQuery, UseDatabaseQuery)
from dumbdb.parser.parser import Parser
from dumbdb.parser.tokenizer import Tokenizer


@dataclass
class Executor:
    dbms: DBMS

    def execute_query(self, query: Query) -> QueryResult:
        operations = {
            CreateDatabaseQuery: self.execute_create_database_query,
            ShowDatabasesQuery: self.execute_show_databases_query,
            DropDatabaseQuery: self.execute_drop_database_query,
            UseDatabaseQuery: self.execute_use_database_query,
            CreateTableQuery: self.execute_create_table_query,
            ShowTablesQuery: self.execute_show_tables_query,
            DropTableQuery: self.execute_drop_table_query,
            SelectQuery: self.execute_select_query,
            InsertQuery: self.execute_insert_query
        }

        if type(query) not in operations:
            raise Exception(f"Query type {type(query)} not supported.")

        return operations[type(query)](query)

    def execute_create_database_query(self, query: CreateDatabaseQuery) -> QueryResult:
        return self.dbms.create_database(query.database)

    def execute_show_databases_query(self, query: ShowDatabasesQuery) -> QueryResult:
        return self.dbms.show_databases()

    def execute_drop_database_query(self, query: DropDatabaseQuery) -> QueryResult:
        return self.dbms.drop_database(query.database)

    def execute_use_database_query(self, query: UseDatabaseQuery) -> QueryResult:
        return self.dbms.use_database(query.database)

    def execute_create_table_query(self, query: CreateTableQuery) -> QueryResult:
        return self.dbms.create_table(query.table.name, query.columns)

    def execute_show_tables_query(self, query: ShowTablesQuery) -> QueryResult:
        return self.dbms.show_tables()

    def execute_drop_table_query(self, query: DropTableQuery) -> QueryResult:
        return self.dbms.drop_table(query.table.name)

    def execute_select_query(self, query: SelectQuery) -> QueryResult:
        return self.dbms.query(query.table.name, {}, query.where_clause)

    def execute_insert_query(self, query: InsertQuery) -> QueryResult:
        return self.dbms.insert(query.table.name, query.row)


@dataclass
class DBEngine:
    dbms: DBMS = field(default_factory=AppendOnlyDBMSWithHashIndexes)
    parser: Parser = field(default_factory=Parser)
    executor: Executor = field(init=False)

    def __post_init__(self):
        self.executor = Executor(self.dbms)

    def cli(self):
        """
        Command line interface for the DBEngine.
        """
        while True:
            try:
                command = input("> ")
                if command.lower() == "exit":
                    break
                result = self.execute_query(command)
                print(result)
            except Exception as e:
                print(f"Error: {str(e)}")
                print("Stack trace:")
                traceback.print_exc()

    def execute_query(self, query: str) -> QueryResult:
        """
        Execute a query passed in as a string.
        1) Tokenize the query.
        2) Parse the tokens into an AST.
        3) Execute the AST.
        """
        tokens = Tokenizer().tokenize(query)
        ast = self.parser.parse(tokens)
        if ast is None:
            raise Exception("Invalid query.")

        return self.executor.execute_query(ast)

    def execute_script(self, script: str) -> QueryResult:
        """
        Execute a script passed in as a string.
        Single queries are separated by a semicolon.
        We keep the semicolon at the end of each query so that the parser works
        correctly.
        """
        queries = [query.strip() + ";" for query in script.split(";")
                   if query.strip()]
        for query in queries:
            self.execute_query(query)


if __name__ == "__main__":
    db_engine = DBEngine()
    db_engine.cli()
