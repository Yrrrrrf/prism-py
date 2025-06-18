# src/prism/db/models.py
"""Database model management and metadata loading."""

import logging
from dataclasses import dataclass, field
from enum import Enum as PyEnum
from typing import Any, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel, Field, create_model
from rich.table import Table
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import Table as SQLTable
from sqlalchemy import inspect, text
from sqlalchemy.orm import DeclarativeBase, declared_attr

from prism.common.types import (
    ArrayType,
    EnumInfo,
    FunctionMetadata,
    FunctionParameter,
    FunctionType,
    JSONBType,
    ObjectType,
    PrismBaseModel,
    get_eq_type,
)
from prism.db.client import DbClient
from prism.ui import console

# For internal logging, not user-facing output
log = logging.getLogger(__name__)


class BaseSQLModel(DeclarativeBase):
    """Base class for all generated SQLAlchemy models."""

    @declared_attr
    def __tablename__(cls) -> str:
        """Generate table name from class name."""
        return cls.__name__.lower()

    @classmethod
    def get_fields(cls) -> Dict[str, Any]:
        """Get all model fields."""
        return {column.name: column for column in cls.__table__.columns}


@dataclass
class ModelManager:
    """Manages model generation and caching for database entities."""

    db_client: DbClient
    include_schemas: List[str]
    exclude_tables: List[str] = field(default_factory=list)

    # Cache dictionaries for database objects
    table_cache: Dict[str, Tuple[SQLTable, Tuple[Type[BaseModel], Type[Any]]]] = field(
        default_factory=dict
    )
    view_cache: Dict[str, Tuple[SQLTable, Tuple[Type[BaseModel], Type[BaseModel]]]] = (
        field(default_factory=dict)
    )
    enum_cache: Dict[str, EnumInfo] = field(default_factory=dict)
    fn_cache: Dict[str, FunctionMetadata] = field(default_factory=dict)
    proc_cache: Dict[str, FunctionMetadata] = field(default_factory=dict)
    trig_cache: Dict[str, FunctionMetadata] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize by loading all models."""
        with console.status("[bold green]Analyzing database schema..."):
            # todo: Add another with status here! To handle each schema...
            # And at the end print the total number of models, enums, views, etc.
            for schema in self.include_schemas:
                console.print(f"Processing schema: [cyan bold]{schema}[/]")
                self._load_models(schema)
                self._load_enums(schema)
                self._load_views(schema)
                self._load_enums(schema)
                # Call the new, separated discovery methods per schema
                self._load_functions_for_schema(schema)
                self._load_procedures_for_schema(schema)
                self._load_triggers_for_schema(schema)

                print()

    def _load_models(self, schema: str) -> None:
        """Load database tables into models."""
        metadata = self.db_client.metadata
        engine = self.db_client.engine

        for table in metadata.tables.values():
            if (
                table.name in inspect(engine).get_table_names(schema=schema)
                and table.name not in self.exclude_tables
            ):
                sample_data = self._get_sample_data(schema, table.name)
                fields = {}
                for column in table.columns:
                    field_type = get_eq_type(
                        str(column.type),
                        sample_data.get(column.name) if sample_data else None,
                        nullable=column.nullable,
                    )
                    if isinstance(field_type, JSONBType):
                        model = field_type.get_model(f"{table.name}_{column.name}")
                        if (
                            sample_data
                            and column.name in sample_data
                            and isinstance(sample_data[column.name], list)
                        ):
                            fields[column.name] = (
                                List[model]
                                if not column.nullable
                                else Optional[List[model]],
                                Field(
                                    default_factory=list
                                    if not column.nullable
                                    else None
                                ),
                            )
                        else:
                            fields[column.name] = (
                                model if not column.nullable else Optional[model],
                                Field(default=... if not column.nullable else None),
                            )
                    elif isinstance(field_type, ArrayType):
                        fields[column.name] = (
                            List[field_type.item_type]
                            if not column.nullable
                            else Optional[List[field_type.item_type]],
                            Field(
                                default_factory=list if not column.nullable else None
                            ),
                        )
                    else:
                        fields[column.name] = (
                            field_type if not column.nullable else Optional[field_type],
                            Field(default=... if not column.nullable else None),
                        )
                pydantic_model = create_model(
                    f"Pydantic_{table.name}", __base__=PrismBaseModel, **fields
                )
                sqlalchemy_model = type(
                    f"SQLAlchemy_{table.name}",
                    (BaseSQLModel,),
                    {
                        "__table__": table,
                        "__tablename__": table.name,
                        "__mapper_args__": {
                            "primary_key": [
                                col for col in table.columns if col.primary_key
                            ]
                        },
                    },
                )
                key = f"{schema}.{table.name}"
                self.table_cache[key] = (table, (pydantic_model, sqlalchemy_model))
                console.print(
                    f"\t[dim]table: [blue]{table.name}[/]"
                )

    def _load_enums(self, schema: str) -> None:
        """Load database enums."""
        for table in self.db_client.metadata.tables.values():
            if (
                table.name
                in inspect(self.db_client.engine).get_table_names(schema=schema)
                and table.name not in self.exclude_tables
            ):
                for column in table.columns:
                    if isinstance(column.type, SQLAlchemyEnum):
                        enum_name = f"{column.name}_enum"
                        if enum_name not in self.enum_cache:
                            self.enum_cache[enum_name] = EnumInfo(
                                name=enum_name,
                                values=list(column.type.enums),
                                python_enum=PyEnum(
                                    enum_name, {v: v for v in column.type.enums}
                                ),
                                schema=schema,
                            )
                            console.print(
                                f"\t[dim]enum: [yellow]{enum_name}[/]"
                            )

    def _load_views(self, schema: str) -> None:
        """Load database views."""
        metadata = self.db_client.metadata
        engine = self.db_client.engine
        for table in metadata.tables.values():
            if table.name in inspect(engine).get_view_names(schema=schema):
                sample_data = self._get_sample_data(schema, table.name)
                query_fields, response_fields = {}, {}
                for column in table.columns:
                    field_type = get_eq_type(
                        str(column.type),
                        sample_data.get(column.name) if sample_data else None,
                        nullable=column.nullable,
                    )
                    query_fields[column.name] = (Optional[str], Field(default=None))
                    if isinstance(field_type, JSONBType):
                        model = field_type.get_model(f"{table.name}_{column.name}")
                        response_fields[column.name] = (
                            Optional[model] if column.nullable else model,
                            Field(default=None),
                        )
                    elif isinstance(field_type, ArrayType):
                        response_fields[column.name] = (
                            List[field_type.item_type],
                            Field(default_factory=list),
                        )
                    else:
                        query_fields[column.name] = (
                            Optional[field_type],
                            Field(default=None),
                        )
                        response_fields[column.name] = (
                            field_type,
                            Field(default=None),
                        )
                QueryModel = create_model(
                    f"View_{table.name}_QueryParams",
                    __base__=PrismBaseModel,
                    **query_fields,
                )
                ResponseModel = create_model(
                    f"View_{table.name}", __base__=PrismBaseModel, **response_fields
                )
                key = f"{schema}.{table.name}"
                self.view_cache[key] = (table, (QueryModel, ResponseModel))
                console.print(
                    f"\t[dim]view: [green]{table.name}[/]"
                )

    def _get_sample_data(
        self, schema: str, table_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get a sample row from the table for type inference."""
        try:
            with next(self.db_client.get_db()) as db:
                query = f"SELECT * FROM {schema}.{table_name} LIMIT 1"
                result = db.execute(text(query)).first()
                if result:
                    return dict(result._mapping)
        except Exception as e:
            log.debug(f"Could not get sample data for {schema}.{table_name}: {e}")
        return None



    def _load_functions_for_schema(self, schema: str) -> None:
        """Load database functions for a specific schema."""
        query = """
            WITH function_info AS (
                SELECT
                    n.nspname as schema,
                    p.proname as name,
                    pg_get_function_identity_arguments(p.oid) as arguments,
                    COALESCE(pg_get_function_result(p.oid), 'void') as return_type,
                    p.provolatile as volatility,
                    p.prosecdef as security_definer,
                    p.proisstrict as is_strict,
                    d.description,
                    p.proretset as returns_set,
                    p.prokind as kind,
                    CASE
                        WHEN p.prorettype = 'trigger'::regtype::oid THEN 'trigger'
                        WHEN p.prokind = 'p' THEN 'procedure'
                        ELSE 'function'
                    END as object_type
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                LEFT JOIN pg_description d ON p.oid = d.objoid
                WHERE n.nspname = :schema
                AND p.prokind IN ('f', 'a', 'w') -- f: normal, a: aggregate, w: window (exclude procedures)
                AND NOT EXISTS (
                    -- Exclude functions that are part of an extension
                    SELECT 1
                    FROM pg_depend dep
                    JOIN pg_extension ext ON dep.refobjid = ext.oid
                    WHERE dep.objid = p.oid
                )
            )
            SELECT * FROM function_info
            ORDER BY name;
        """

        def determine_function_type(row: Any) -> FunctionType:
            if row.returns_set:
                return FunctionType.SET_RETURNING
            if "TABLE" in (row.return_type or ""):
                return FunctionType.TABLE
            if row.kind == "a":
                return FunctionType.AGGREGATE
            if row.kind == "w":
                return FunctionType.WINDOW
            return FunctionType.SCALAR


        with next(self.db_client.get_db()) as db:
            result = db.execute(text(query), {"schema": schema})
            for row in result:
                metadata = FunctionMetadata(
                    schema=row.schema,
                    name=row.name,
                    return_type=row.return_type or "void",
                    parameters=parse_parameters(row.arguments),
                    type=determine_function_type(row),
                    object_type=ObjectType.FUNCTION,
                    is_strict=row.is_strict,
                    description=row.description,
                )
                component = f"{row.schema}.{row.name}"
                self.fn_cache[component] = metadata
                console.print(
                    f"\t[dim]function: [red]{row.name}[/]"
                )


    def _load_procedures_for_schema(self, schema: str) -> None:
        """Load database procedures for a specific schema."""
        query = """
            WITH procedure_info AS (
                SELECT
                    n.nspname as schema,
                    p.proname as name,
                    pg_get_function_identity_arguments(p.oid) as arguments,
                    COALESCE(pg_get_function_result(p.oid), 'void') as return_type,
                    p.provolatile as volatility,
                    p.prosecdef as security_definer,
                    p.proisstrict as is_strict,
                    d.description,
                    p.proretset as returns_set,
                    p.prokind as kind,
                    'procedure' as object_type
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                LEFT JOIN pg_description d ON p.oid = d.objoid
                WHERE n.nspname = :schema
                AND p.prokind = 'p' -- p: procedure
                AND NOT EXISTS (
                    -- Exclude procedures that are part of an extension
                    SELECT 1
                    FROM pg_depend dep
                    JOIN pg_extension ext ON dep.refobjid = ext.oid
                    WHERE dep.objid = p.oid
                )
            )
            SELECT * FROM procedure_info
            ORDER BY name;
        """
        with next(self.db_client.get_db()) as db:
            result = db.execute(text(query), {"schema": schema})
            for row in result:
                metadata = FunctionMetadata(
                    schema=row.schema,
                    name=row.name,
                    return_type=row.return_type or "void",
                    parameters=parse_parameters(row.arguments),
                    type=FunctionType.SCALAR,  # Procedures are typically scalar
                    object_type=ObjectType.PROCEDURE,
                    is_strict=row.is_strict,
                    description=row.description,
                )
                component = f"{row.schema}.{row.name}"
                self.proc_cache[component] = metadata
                console.print(
                    f"\t[dim]procedure: [yellow]{row.name}[/]"
                )


    def _load_triggers_for_schema(self, schema: str) -> None:
        """Load database triggers for a specific schema."""
        query = """
            WITH trigger_info AS (
                SELECT
                    n.nspname as schema,
                    p.proname as name,
                    pg_get_function_identity_arguments(p.oid) as arguments,
                    COALESCE(pg_get_function_result(p.oid), 'trigger') as return_type,
                    p.provolatile as volatility,
                    p.prosecdef as security_definer,
                    p.proisstrict as is_strict,
                    d.description,
                    p.proretset as returns_set,
                    p.prokind as kind,
                    'trigger' as object_type
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                LEFT JOIN pg_description d ON p.oid = d.objoid
                WHERE n.nspname = :schema
                AND p.prorettype = 'trigger'::regtype::oid -- Only trigger functions
                AND NOT EXISTS (
                    -- Exclude triggers that are part of an extension
                    SELECT 1
                    FROM pg_depend dep
                    JOIN pg_extension ext ON dep.refobjid = ext.oid
                    WHERE dep.objid = p.oid
                )
            )
            SELECT * FROM trigger_info
            ORDER BY name;
        """


        with next(self.db_client.get_db()) as db:
            result = db.execute(text(query), {"schema": schema})
            for row in result:
                metadata = FunctionMetadata(
                    schema=row.schema,
                    name=row.name,
                    return_type=row.return_type or "trigger",
                    parameters=parse_parameters(row.arguments),
                    type=FunctionType.SCALAR,  # Triggers are typically scalar
                    object_type=ObjectType.TRIGGER,
                    is_strict=row.is_strict,
                    description=row.description,
                )
                component = f"{row.schema}.{row.name}"
                self.trig_cache[component] = metadata
                console.print(
                    f"\t[dim]trigger:  [orange1]{row.name}[/]"
                )
















    def log_metadata_stats(self):
        """Print metadata statistics in a rich table format."""
        stats = {}
        for schema in sorted(self.include_schemas):
            stats[schema] = {
                "tables": sum(
                    1 for k in self.table_cache if k.startswith(schema + ".")
                ),
                "views": sum(1 for k in self.view_cache if k.startswith(schema + ".")),
                "enums": sum(1 for e in self.enum_cache.values() if e.schema == schema),
                "functions": sum(
                    1 for f in self.fn_cache.values() if f.schema == schema
                ),
                "procedures": sum(
                    1 for p in self.proc_cache.values() if p.schema == schema
                ),
                "triggers": sum(
                    1 for t in self.trig_cache.values() if t.schema == schema
                ),
            }

        console.rule("[bold]ModelManager Statistics")
        table = Table(header_style="bold", show_footer=True)
        table.add_column("Schema", style="cyan", no_wrap=True, footer="[bold]TOTAL[/]")
        table.add_column("Tables", style="blue", justify="right")
        table.add_column("Views", style="green", justify="right")
        table.add_column("Enums", style="magenta", justify="right")
        table.add_column("Functions", style="red", justify="right")
        table.add_column("Procedures", style="yellow", justify="right")
        table.add_column("Triggers", style="orange1", justify="right")
        table.add_column("Total", justify="right", style="bold")

        totals = {key: 0 for key in stats.get(self.include_schemas[0], {}).keys()}
        for schema, counts in stats.items():
            schema_total = sum(counts.values())
            table.add_row(
                schema,
                str(counts["tables"]),
                str(counts["views"]),
                str(counts["enums"]),
                str(counts["functions"]),
                str(counts["procedures"]),
                str(counts["triggers"]),
                str(schema_total),
            )
            for key in totals:
                totals[key] += counts.get(key, 0)

        table.columns[1].footer = f"[blue]{totals['tables']}[/]"
        table.columns[2].footer = f"[green]{totals['views']}[/]"
        table.columns[3].footer = f"[magenta]{totals['enums']}[/]"
        table.columns[4].footer = f"[red]{totals['functions']}[/]"
        table.columns[5].footer = f"[yellow]{totals['procedures']}[/]"
        table.columns[6].footer = f"[orange1]{totals['triggers']}[/]"
        table.columns[7].footer = f"[bold]{sum(totals.values())}[/]"

        console.print(table)



def parse_parameters(args_str: str) -> List[FunctionParameter]:
    if not args_str:
        return []
    parameters = []
    for arg in args_str.split(", "):
        parts = arg.split()
        if not parts:
            continue

        # Default mode is IN if not specified
        mode = "IN"
        if parts[0].upper() in ("IN", "OUT", "INOUT", "VARIADIC"):
            mode = parts.pop(0).upper()

        # The last part is the type, the rest is the name
        param_type = parts.pop(-1)
        param_name = " ".join(parts) if parts else ""

        parameters.append(
            FunctionParameter(name=param_name, type=param_type, mode=mode)
        )
    return parameters
