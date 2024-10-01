import contextlib
from typing import Any
from pprint import pprint
from collections.abc import Iterable, Mapping

import flightsql
from flightsql.dbapi import Connection

import ibis
from ibis.backends.sql import SQLBackend
import ibis.backends.sql.compilers as sc
import ibis.expr.schema as sch
from ibis.backends.sql.compilers.base import C
import ibis.expr.types as ir

import sqlglot as sg
import sqlglot.expressions as sge


class MyBackend(SQLBackend):
    name = "secret"
    compiler = sc.DataFusionCompiler()
    dialect = "datafusion"
    supports_arrays = True
    supports_create_or_replace = False
    supports_temporary_tables = False

    def do_connect(self, client: flightsql.FlightSQLClient, **kwargs) -> None:
        self.con = flightsql.connect(client, **kwargs)

    @classmethod
    def from_connection(cls, con: Connection):
        new = cls()
        new.con = con
        return new

    def create_table(self, *args, **kwargs):
        raise "unimplemented"

    def version(self) -> str:
        "unknown"

    def list_tables(
        self,
        like: str | None = None,
        database: tuple[str, str] | str | None = None,
    ) -> list[str]:
        table_loc = self._to_sqlglot_table(database)

        catalog = table_loc.catalog or self.current_catalog
        database = table_loc.db or self.current_database

        col = "table_name"
        sql = (
            sg.select(col)
            .from_(sg.table("tables", db="information_schema"))
            .distinct()
            .where(
                C.table_catalog.isin(sge.convert(catalog), sge.convert("temp")),
                C.table_schema.eq(sge.convert(database)),
            )
            .sql(self.dialect)
        )
        out = self.con.execute(sql).fetch_arrow_table()

        return self._filter_with_like(out[col].to_pylist(), like)

    def raw_sql(self, query: str | sg.Expression, **kwargs: Any) -> Any:
        """Execute a raw SQL query."""
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)

        return self.con.execute(query, **kwargs)

    @contextlib.contextmanager
    def _safe_raw_sql(self, *args, **kwargs):
        yield self.raw_sql(*args, **kwargs)

    def to_pyarrow(self, expr: ir.Expr, **kwargs: Any):
        table_expr = expr.as_table()
        raw_sql = self.compile(table_expr, **kwargs)

        info = self.con.client.execute(raw_sql)
        reader = self.con.client.do_get(info.endpoints[0].ticket)

        return expr.__pyarrow_result__(reader.read_all())

    def execute(
        self,
        expr: ir.Expr,
        **kwargs: Any,
    ) -> Any:
        """Execute an expression."""
        table_expr = expr.as_table()
        raw_sql = self.compile(table_expr, **kwargs)

        info = self.con.client.execute(raw_sql)
        reader = self.con.client.do_get(info.endpoints[0].ticket)

        return expr.__pandas_result__(reader.read_pandas(timestamp_as_object=True))

    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        sql = (
            sg.select("*", dialect=self.dialect)
            .from_(sg.table(table=table_name, catalog=catalog, db=database))
            .sql(dialect=self.dialect)
        )
        return self._get_schema_using_query(sql)

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        return sch.from_pyarrow_schema(self.con.client.execute(query).schema)


if __name__ == "__main__":
    client = flightsql.FlightSQLClient(host="localhost", port=50051, insecure=True)

    ibis.options.verbose = True

    conn = MyBackend.from_connection(flightsql.connect(client))

    trades = conn.table("trades_v2", database="external.us_stocks_all")

    print(trades.execute(limit=100))
