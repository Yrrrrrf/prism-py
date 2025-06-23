# src/prism/core/introspection/postgres.py
from typing import Dict, List

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from ..models.tables import ColumnMetadata, ColumnReference, TableMetadata
from ..models.enums import EnumInfo
from ..models.functions import FunctionMetadata
from .base import IntrospectorABC


class PostgresIntrospector(IntrospectorABC):
    """Introspector implementation for PostgreSQL databases."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self.inspector = inspect(engine)

    def get_schemas(self) -> List[str]:
        return self.inspector.get_schema_names()

    def get_tables(self, schema: str) -> List[TableMetadata]:
        results: List[TableMetadata] = []
        table_names = self.inspector.get_table_names(schema=schema)
        view_names = self.inspector.get_view_names(schema=schema)

        for table_name in table_names + view_names:
            is_view = table_name in view_names
            columns = self._get_columns(schema, table_name)
            pks = self.inspector.get_pk_constraint(table_name, schema).get("constrained_columns", [])

            results.append(
                TableMetadata(
                    name=table_name,
                    schema=schema,
                    columns=columns,
                    primary_key_columns=pks,
                    is_view=is_view,
                    comment=self.inspector.get_table_comment(table_name, schema).get("text")
                )
            )
        return results

    def _get_columns(self, schema: str, table_name: str) -> List[ColumnMetadata]:
        column_data = self.inspector.get_columns(table_name, schema)
        fks = self.inspector.get_foreign_keys(table_name, schema)
        fk_map = {item['constrained_columns'][0]: item for item in fks}

        columns = []
        for col in column_data:
            foreign_key = None
            if col['name'] in fk_map:
                fk_info = fk_map[col['name']]
                ref_table = fk_info['referred_table']
                ref_schema = fk_info['referred_schema']
                ref_column = fk_info['referred_columns'][0]
                foreign_key = ColumnReference(schema=ref_schema, table=ref_table, column=ref_column)

            columns.append(
                ColumnMetadata(
                    name=col['name'],
                    sql_type=str(col['type']),
                    is_nullable=col['nullable'],
                    is_primary_key=col.get('primary_key', False),
                    default_value=col.get('default'),
                    comment=col.get('comment'),
                    foreign_key=foreign_key,
                )
            )
        return columns

    def get_enums(self, schema: str) -> Dict[str, EnumInfo]:
        # SQLAlchemy's inspector does not have a dedicated get_enums method.
        # This requires a dialect-specific query.
        query = text("""
            SELECT t.typname AS name, array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = :schema AND t.typtype = 'e'
            GROUP BY t.typname;
        """)
        with self.engine.connect() as connection:
            result = connection.execute(query, {"schema": schema})
            return {
                row.name: EnumInfo(name=row.name, schema=schema, values=row.values)
                for row in result
            }

    def get_functions(self, schema: str) -> List[FunctionMetadata]:
        # TODO: Implement the SQL query to fetch function/procedure metadata
        # from pg_proc, similar to the old implementation.
        return []