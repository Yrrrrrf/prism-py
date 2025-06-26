# src/prism/core/introspection/postgres.py
from typing import Any, Dict, List, Tuple

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from prism.core.models.enums import EnumInfo
from prism.core.models.functions import (
    FunctionMetadata,
    FunctionParameter,
    FunctionType,
    ObjectType,
)
from prism.core.models.tables import ColumnMetadata, ColumnReference, TableMetadata
from prism.core.introspection.base import IntrospectorABC


def _parse_parameters(args_str: str) -> List[FunctionParameter]:
    """Parses a PostgreSQL function argument string into a list of FunctionParameter objects."""
    if not args_str:
        return []
    parameters = []
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
        self._all_enums_cache: Dict[str, EnumInfo] | None = None
        self._column_type_map_cache: Dict[str, Dict[Tuple[str, str], str]] = {}

    def _get_column_true_types(self, schema: str) -> Dict[Tuple[str, str], str]:
        """
        Gets the true user-defined type name for each column in a schema,
        bypassing SQLAlchemy's type string representation which can be misleading.
        Returns a map of {(table_name, column_name): true_type_name}.
        """
        if schema in self._column_type_map_cache:
            return self._column_type_map_cache[schema]

        query = text(
            """
            SELECT
                c.relname AS table_name,
                a.attname AS column_name,
                t.typname AS type_name
            FROM
                pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON t.oid = a.atttypid
            WHERE
                n.nspname = :schema
                AND a.attnum > 0
                AND NOT a.attisdropped;
            """
        )
        type_map: Dict[Tuple[str, str], str] = {}
        with self.engine.connect() as connection:
            result = connection.execute(query, {"schema": schema})
            for row in result:
                type_map[(row.table_name, row.column_name)] = row.type_name

        self._column_type_map_cache[schema] = type_map
        return type_map

    def _get_all_enums(self) -> Dict[str, EnumInfo]:
        """Fetches all user-defined enums across all schemas and caches the result."""
        if self._all_enums_cache is not None:
            return self._all_enums_cache

        query = text(
            """
            SELECT n.nspname AS schema, t.typname AS name,
                   array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND t.typtype = 'e'
              AND NOT EXISTS (
                  SELECT 1 FROM pg_depend dep JOIN pg_extension ext ON dep.refobjid = ext.oid
                  WHERE dep.objid = t.oid
              )
            GROUP BY n.nspname, t.typname;
            """
        )
        enums_map: Dict[str, EnumInfo] = {}
        with self.engine.connect() as connection:
            result = connection.execute(query)
            for row in result:
                qualified_name = f"{row.schema}.{row.name}"
                enums_map[qualified_name] = EnumInfo(
                    name=row.name, schema=row.schema, values=row.values
                )
        self._all_enums_cache = enums_map
        return self._all_enums_cache

    def get_schemas(self) -> List[str]:
        """Returns a list of all user-defined schema names, excluding system schemas."""
        all_schemas = self.inspector.get_schema_names()
        SYSTEM_SCHEMAS_TO_EXCLUDE = {
            "information_schema",
            "pg_catalog",
            "pg_toast",
        }
        return [s for s in all_schemas if s not in SYSTEM_SCHEMAS_TO_EXCLUDE]

    def get_enums(self, schema: str) -> Dict[str, EnumInfo]:
        """Returns enum definitions for a specific schema by filtering the global map."""
        all_enums = self._get_all_enums()
        return {
            info.name: info
            for qualified_name, info in all_enums.items()
            if info.schema == schema
        }

    def _create_table_metadata(
        self, schema: str, name: str, is_view: bool, all_enums_map: Dict[str, EnumInfo]
    ) -> TableMetadata:
        """Private helper to build a TableMetadata object for a table or view."""
        column_true_types = self._get_column_true_types(schema)

        pk_constraint = self.inspector.get_pk_constraint(name, schema)
        pk_column_names = pk_constraint.get("constrained_columns", [])
        column_data = self.inspector.get_columns(name, schema)
        fks = self.inspector.get_foreign_keys(name, schema)
        fk_map = {item["constrained_columns"][0]: item for item in fks}

        columns = []
        for col in column_data:
            is_primary_key = col["name"] in pk_column_names

            foreign_key = None
            if col["name"] in fk_map:
                fk_info = fk_map[col["name"]]
                ref_table = fk_info["referred_table"]
                ref_schema = fk_info["referred_schema"]
                ref_column = fk_info["referred_columns"][0]
                foreign_key = ColumnReference(
                    schema=ref_schema, table=ref_table, column=ref_column
                )

            true_type_name = column_true_types.get(
                (name, col["name"]), str(col["type"])
            )
            enum_info = all_enums_map.get(f"{schema}.{true_type_name}")

            final_sql_type = enum_info.name if enum_info else true_type_name

            columns.append(
                ColumnMetadata(
                    name=col["name"],
                    sql_type=final_sql_type,
                    is_nullable=col["nullable"],
                    is_pk=is_primary_key,
                    default_value=col.get("default"),
                    comment=col.get("comment"),
                    foreign_key=foreign_key,
                    enum_info=enum_info,
                )
            )

        comment = self.inspector.get_table_comment(name, schema).get("text")
        return TableMetadata(
            name=name,
            schema=schema,
            columns=columns,
            primary_key_columns=pk_column_names,
            is_view=is_view,
            comment=comment,
        )

    def get_tables(self, schema: str) -> List[TableMetadata]:
        """Returns metadata for all tables in a given schema."""
        table_names = self.inspector.get_table_names(schema=schema)
        all_enums = self._get_all_enums()
        return [
            self._create_table_metadata(
                schema, name, is_view=False, all_enums_map=all_enums
            )
            for name in table_names
        ]

    def get_views(self, schema: str) -> List[TableMetadata]:
        """Returns metadata for all views in a given schema."""
        view_names = self.inspector.get_view_names(schema=schema)
        all_enums = self._get_all_enums()
        return [
            self._create_table_metadata(
                schema, name, is_view=True, all_enums_map=all_enums
            )
            for name in view_names
        ]

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
                    p.prorettype,
                    p.proretset AS returns_set, p.prokind AS kind, d.description
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                LEFT JOIN pg_description d ON p.oid = d.objoid
                WHERE n.nspname = :schema AND {kind_filter} AND NOT EXISTS (
                    SELECT 1 FROM pg_depend dep JOIN pg_extension ext ON dep.refobjid = ext.oid
                    WHERE dep.objid = p.oid
                )
            )
            SELECT * FROM func_info
            WHERE prorettype NOT IN (
                'anyelement'::regtype, 'anyarray'::regtype, 'anynonarray'::regtype,
                'anyenum'::regtype, 'anyrange'::regtype, 'record'::regtype,
                'trigger'::regtype, 'event_trigger'::regtype, 'internal'::regtype
            )
            ORDER BY name;
        """)
        results = []
        with self.engine.connect() as connection:
            rows = connection.execute(query, {"schema": schema}).mappings().all()
            for row in rows:
                if row["kind"] == "p":
                    obj_type, func_type = ObjectType.PROCEDURE, FunctionType.SCALAR
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
        # The filter `prorettype != 'void'::regtype` is added to exclude procedures.
        return self._fetch_callable_metadata(
            schema,
            "p.prokind IN ('f', 'a', 'w') AND p.prorettype != 'void'::regtype",
        )

    def get_procedures(self, schema: str) -> List[FunctionMetadata]:
        """Returns metadata for all procedures."""
        query = text("""
            SELECT
                n.nspname AS schema, p.proname AS name,
                pg_get_function_identity_arguments(p.oid) AS arguments,
                d.description
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            LEFT JOIN pg_description d ON p.oid = d.objoid
            WHERE n.nspname = :schema AND p.prokind = 'p' AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep JOIN pg_extension ext ON dep.refobjid = ext.oid
                WHERE dep.objid = p.oid
            )
            ORDER BY name;
        """)
        results = []
        with self.engine.connect() as connection:
            rows = connection.execute(query, {"schema": schema}).mappings().all()
            for row in rows:
                results.append(
                    FunctionMetadata(
                        schema=row["schema"],
                        name=row["name"],
                        return_type="void",
                        parameters=_parse_parameters(row["arguments"]),
                        type=FunctionType.SCALAR,
                        object_type=ObjectType.PROCEDURE,
                        description=row["description"],
                    )
                )
        return results

    def get_triggers(self, schema: str) -> List[FunctionMetadata]:
        """Returns metadata for all trigger functions."""
        query = text("""
            SELECT
                n.nspname AS schema, p.proname AS name,
                pg_get_function_identity_arguments(p.oid) AS arguments,
                d.description
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            LEFT JOIN pg_description d ON p.oid = d.objoid
            WHERE n.nspname = :schema AND p.prorettype = 'trigger'::regtype::oid
            AND NOT EXISTS (
                SELECT 1 FROM pg_depend dep JOIN pg_extension ext ON dep.refobjid = ext.oid
                WHERE dep.objid = p.oid
            )
            ORDER BY name;
        """)
        results = []
        with self.engine.connect() as connection:
            rows = connection.execute(query, {"schema": schema}).mappings().all()
            for row in rows:
                results.append(
                    FunctionMetadata(
                        schema=row["schema"],
                        name=row["name"],
                        return_type="trigger",
                        parameters=_parse_parameters(row["arguments"]),
                        type=FunctionType.SCALAR,
                        object_type=ObjectType.TRIGGER,
                        description=row["description"],
                    )
                )
        return results
