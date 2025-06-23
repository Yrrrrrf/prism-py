# src/prism/core/introspection/postgres.py
from typing import Any, Dict, List

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from ..models.enums import EnumInfo
from ..models.functions import (
    FunctionMetadata,
    FunctionParameter,
    FunctionType,
    ObjectType,
)
from ..models.tables import ColumnMetadata, ColumnReference, TableMetadata
from .base import IntrospectorABC


def _parse_parameters(args_str: str) -> List[FunctionParameter]:
    """Parses a PostgreSQL function argument string into a list of FunctionParameter objects."""
    if not args_str:
        return []
    parameters = []
    # This is a simplified parser; it might not handle all edge cases.
    for arg in args_str.split(", "):
        parts = arg.strip().split()
        if not parts:
            continue

        mode = "IN"
        if parts[0].upper() in ("IN", "OUT", "INOUT", "VARIADIC"):
            mode = parts.pop(0).upper()

        default_value = None
        has_default = "DEFAULT" in " ".join(parts).upper()
        if has_default:
            name_and_type, default_expr = " ".join(parts).split(" DEFAULT ", 1)
            parts = name_and_type.split()
            default_value = default_expr.strip()

        param_type = parts.pop(-1)
        param_name = " ".join(parts) if parts else ""

        parameters.append(
            FunctionParameter(
                name=param_name,
                type=param_type,
                mode=mode,
                has_default=has_default,
                default_value=default_value,
            )
        )
    return parameters


class PostgresIntrospector(IntrospectorABC):
    """Introspector implementation for PostgreSQL databases."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self.inspector = inspect(engine)

    # --- get_schemas, get_tables, _get_columns, get_enums remain unchanged ---
    def get_schemas(self) -> List[str]:
        return self.inspector.get_schema_names()

    def _create_table_metadata(
        self, schema: str, name: str, is_view: bool
    ) -> TableMetadata:
        """Private helper to build a TableMetadata object for a table or view."""
        columns = self._get_columns(schema, name)
        # Views don't have primary keys, so this will correctly return an empty list
        pks = self.inspector.get_pk_constraint(name, schema).get(
            "constrained_columns", []
        )
        comment = self.inspector.get_table_comment(name, schema).get("text")

        return TableMetadata(
            name=name,
            schema=schema,
            columns=columns,
            primary_key_columns=pks,
            is_view=is_view,
            comment=comment,
        )

    def get_tables(self, schema: str) -> List[TableMetadata]:
        """Returns metadata for all tables (excluding views) in a given schema."""
        table_names = self.inspector.get_table_names(schema=schema)
        return [
            self._create_table_metadata(schema, name, is_view=False)
            for name in table_names
        ]

    def get_views(self, schema: str) -> List[TableMetadata]:
        """Returns metadata for all views in a given schema."""
        view_names = self.inspector.get_view_names(schema=schema)
        return [
            self._create_table_metadata(schema, name, is_view=True)
            for name in view_names
        ]

    def _get_columns(self, schema: str, table_name: str) -> List[ColumnMetadata]:
        column_data = self.inspector.get_columns(table_name, schema)
        fks = self.inspector.get_foreign_keys(table_name, schema)
        fk_map = {item["constrained_columns"][0]: item for item in fks}
        columns = []
        for col in column_data:
            foreign_key = None
            if col["name"] in fk_map:
                fk_info = fk_map[col["name"]]
                ref_table = fk_info["referred_table"]
                ref_schema = fk_info["referred_schema"]
                ref_column = fk_info["referred_columns"][0]
                foreign_key = ColumnReference(
                    schema=ref_schema, table=ref_table, column=ref_column
                )
            columns.append(
                ColumnMetadata(
                    name=col["name"],
                    sql_type=str(col["type"]),
                    is_nullable=col["nullable"],
                    is_pk=col.get("primary_key", False),
                    default_value=col.get("default"),
                    comment=col.get("comment"),
                    foreign_key=foreign_key,
                )
            )
        return columns

    def get_enums(self, schema: str) -> Dict[str, EnumInfo]:
        query = text(
            "SELECT t.typname AS name, array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid JOIN pg_namespace n ON t.typnamespace = n.oid WHERE n.nspname = :schema AND t.typtype = 'e' GROUP BY t.typname;"
        )
        with self.engine.connect() as connection:
            result = connection.execute(query, {"schema": schema})
            return {
                row.name: EnumInfo(name=row.name, schema=schema, values=row.values)
                for row in result
            }

    def _fetch_callable_metadata(
        self, schema: str, kind_filter: str
    ) -> List[FunctionMetadata]:
        """Generic method to fetch metadata for functions, procedures, or triggers."""
        query = text(f"""
            WITH func_info AS (
                SELECT
                    p.oid, n.nspname AS schema, p.proname AS name,
                    pg_get_function_identity_arguments(p.oid) AS arguments,
                    COALESCE(pg_get_function_result(p.oid), 'void') AS return_type,
                    p.proretset AS returns_set, p.prokind AS kind, d.description
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                LEFT JOIN pg_description d ON p.oid = d.objoid
                WHERE n.nspname = :schema AND {kind_filter} AND NOT EXISTS (
                    SELECT 1 FROM pg_depend dep JOIN pg_extension ext ON dep.refobjid = ext.oid
                    WHERE dep.objid = p.oid
                )
            )
            SELECT * FROM func_info ORDER BY name;
        """)
        results = []
        with self.engine.connect() as connection:
            rows = connection.execute(query, {"schema": schema}).mappings().all()
            for row in rows:
                if row["kind"] == "p":
                    obj_type, func_type = ObjectType.PROCEDURE, FunctionType.SCALAR
                elif row["return_type"] == "trigger":
                    obj_type, func_type = ObjectType.TRIGGER, FunctionType.SCALAR
                else:
                    obj_type = ObjectType.FUNCTION
                    func_type = (
                        FunctionType.SET_RETURNING
                        if row["returns_set"]
                        else (
                            FunctionType.TABLE
                            if "TABLE" in row["return_type"]
                            else FunctionType.SCALAR
                        )
                    )

                results.append(
                    FunctionMetadata(
                        schema=row["schema"],
                        name=row["name"],
                        return_type=row["return_type"],
                        parameters=_parse_parameters(row["arguments"]),
                        type=func_type,
                        object_type=obj_type,
                        description=row["description"],
                    )
                )
        return results

    def get_functions(self, schema: str) -> List[FunctionMetadata]:
        """Returns metadata for all functions, excluding procedures and triggers."""
        return self._fetch_callable_metadata(
            schema,
            "p.prokind IN ('f', 'a', 'w') AND p.prorettype != 'trigger'::regtype::oid",
        )

    def get_procedures(self, schema: str) -> List[FunctionMetadata]:
        """Returns metadata for all procedures."""
        return self._fetch_callable_metadata(schema, "p.prokind = 'p'")

    def get_triggers(self, schema: str) -> List[FunctionMetadata]:
        """Returns metadata for all trigger functions."""
        return self._fetch_callable_metadata(
            schema, "p.prorettype = 'trigger'::regtype::oid"
        )
