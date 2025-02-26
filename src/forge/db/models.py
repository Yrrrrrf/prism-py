# src/forge/db/models.py
"""Database model management and metadata loading."""

from typing import Dict, List, Tuple, Type, Any, Optional, Union
import json
from dataclasses import dataclass, field
from sqlalchemy import Table, Column, MetaData, inspect, text
from sqlalchemy import Enum as SQLAlchemyEnum
from pydantic import BaseModel, create_model, Field, ConfigDict
from enum import Enum as PyEnum

from forge.core.logging import log, color_palette
from forge.common.types import (
    EnumInfo,
    FunctionParameter,
    FunctionMetadata,
    FunctionType,
    ObjectType,
    ForgeBaseModel,
    ArrayType,
    JSONBType,
    get_eq_type,
    parse_array_type,
    create_dynamic_model,
)
from forge.db.client import DbClient
from sqlalchemy.orm import DeclarativeBase, declared_attr


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
    table_cache: Dict[str, Tuple[Table, Tuple[Type[BaseModel], Type[Any]]]] = field(
        default_factory=dict
    )
    view_cache: Dict[str, Tuple[Table, Tuple[Type[BaseModel], Type[BaseModel]]]] = (
        field(default_factory=dict)
    )
    enum_cache: Dict[str, EnumInfo] = field(default_factory=dict)
    fn_cache: Dict[str, FunctionMetadata] = field(default_factory=dict)
    proc_cache: Dict[str, FunctionMetadata] = field(default_factory=dict)
    trig_cache: Dict[str, FunctionMetadata] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize by loading all models."""
        self._load_models()
        self._load_enums()
        self._load_views()
        self._load_functions()

    def _load_models(self):
        """Load database tables into models."""
        log.info("Loading database tables...")

        metadata = self.db_client.metadata
        engine = self.db_client.engine

        for schema in self.include_schemas:
            log.info(f"Processing schema: {color_palette['schema'](schema)}")

            for table in metadata.tables.values():
                if (
                    table.name in inspect(engine).get_table_names(schema=schema)
                    and table.name not in self.exclude_tables
                ):
                    # Try to get sample data for improved type inference
                    sample_data = self._get_sample_data(schema, table.name)

                    # Create Pydantic model fields
                    fields = {}
                    for column in table.columns:
                        field_type = get_eq_type(
                            str(column.type),
                            sample_data.get(column.name) if sample_data else None,
                            nullable=column.nullable,
                        )

                        # Handle different field types
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
                                    default_factory=list
                                    if not column.nullable
                                    else None
                                ),
                            )
                        else:
                            fields[column.name] = (
                                field_type
                                if not column.nullable
                                else Optional[field_type],
                                Field(default=... if not column.nullable else None),
                            )

                    # Create Pydantic model
                    model_config = ConfigDict(
                        from_attributes=True,
                        arbitrary_types_allowed=True,
                        populate_by_name=True,
                    )

                    pydantic_model = create_model(
                        f"Pydantic_{table.name}", __base__=ForgeBaseModel, **fields
                    )

                    # Create SQLAlchemy model with proper metadata binding
                    sqlalchemy_model = type(
                        f"SQLAlchemy_{table.name}",
                        (BaseSQLModel,),
                        {
                            '__table__': table,
                            '__tablename__': table.name,
                            '__mapper_args__': {
                                'primary_key': [col for col in table.columns if col.primary_key]
                            }
                        }
                    )

                    # Store in cache
                    key = f"{schema}.{table.name}"
                    self.table_cache[key] = (table, (pydantic_model, sqlalchemy_model))
                    log.success(
                        f"Loaded table: {color_palette['schema'](schema)}.{color_palette['table'](table.name)}"
                    )

        log.info(f"Loaded {color_palette['table'](str(len(self.table_cache)))} tables")

    def _load_enums(self):
        """Load database enums."""
        log.info("Loading database enums...")

        for schema in self.include_schemas:
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
                                log.success(
                                    f"Loaded enum: {color_palette['schema'](schema)}.{color_palette['enum'](enum_name)}"
                                )

        log.info(f"Loaded {color_palette['enum'](str(len(self.enum_cache)))} enums")

    def _load_views(self):
        """Load database views."""
        log.info("Loading database views...")

        metadata = self.db_client.metadata
        engine = self.db_client.engine

        for schema in self.include_schemas:
            for table in metadata.tables.values():
                if table.name in inspect(engine).get_view_names(schema=schema):
                    # Try to get sample data for improved type inference
                    sample_data = self._get_sample_data(schema, table.name)

                    # Create query params and response fields
                    query_fields = {}
                    response_fields = {}

                    for column in table.columns:
                        column_type = str(column.type)
                        nullable = column.nullable
                        field_type = get_eq_type(
                            column_type,
                            sample_data.get(column.name) if sample_data else None,
                            nullable=nullable,
                        )

                        # Create query field (always optional)
                        query_fields[column.name] = (Optional[str], Field(default=None))

                        # Create response field based on type
                        if isinstance(field_type, JSONBType):
                            model = field_type.get_model(f"{table.name}_{column.name}")
                            if (
                                sample_data
                                and column.name in sample_data
                                and isinstance(sample_data[column.name], list)
                            ):
                                response_fields[column.name] = (
                                    List[model],
                                    Field(default_factory=list),
                                )
                            else:
                                response_fields[column.name] = (
                                    Optional[model] if nullable else model,
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

                    # Create models
                    QueryModel = create_model(
                        f"View_{table.name}_QueryParams",
                        __base__=ForgeBaseModel,
                        **query_fields,
                    )

                    ResponseModel = create_model(
                        f"View_{table.name}", __base__=ForgeBaseModel, **response_fields
                    )

                    # Store in cache
                    key = f"{schema}.{table.name}"
                    self.view_cache[key] = (table, (QueryModel, ResponseModel))
                    log.success(
                        f"Loaded view: {color_palette['schema'](schema)}.{color_palette['view'](table.name)}"
                    )

        log.info(f"Loaded {color_palette['view'](str(len(self.view_cache)))} views")

    def _load_functions(self):
        """Load database functions, procedures, and triggers."""
        log.info("Loading database functions...")

        fn_cache, proc_cache, trig_cache = self._discover_functions()

        self.fn_cache = fn_cache
        self.proc_cache = proc_cache
        self.trig_cache = trig_cache

        log.info(
            f"Loaded {color_palette['function'](str(len(self.fn_cache)))} functions, "
            f"{color_palette['procedure'](str(len(self.proc_cache)))} procedures, "
            f"{color_palette['trigger'](str(len(self.trig_cache)))} triggers"
        )

    def _discover_functions(self):
        """Discover database functions, procedures, and triggers."""
        function_cache = {}
        procedure_cache = {}
        trigger_cache = {}

        # SQL query to get function information
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
                        WHEN EXISTS (
                            SELECT 1 
                            FROM pg_trigger t 
                            WHERE t.tgfoid = p.oid
                        ) OR p.prorettype = 'trigger'::regtype::oid THEN 'trigger'
                        WHEN p.prokind = 'p' THEN 'procedure'
                        ELSE 'function'
                    END as object_type,
                    -- Get trigger event information if it's a trigger function
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 
                            FROM pg_trigger t 
                            WHERE t.tgfoid = p.oid
                        ) THEN (
                            SELECT string_agg(DISTINCT evt.event_type, ', ')
                            FROM (
                                SELECT 
                                    CASE tg.tgtype::integer & 2::integer 
                                        WHEN 2 THEN 'BEFORE'
                                        ELSE 'AFTER'
                                    END || ' ' ||
                                    CASE 
                                        WHEN tg.tgtype::integer & 4::integer = 4 THEN 'INSERT'
                                        WHEN tg.tgtype::integer & 8::integer = 8 THEN 'DELETE'
                                        WHEN tg.tgtype::integer & 16::integer = 16 THEN 'UPDATE'
                                        WHEN tg.tgtype::integer & 32::integer = 32 THEN 'TRUNCATE'
                                    END as event_type
                                FROM pg_trigger tg
                                WHERE tg.tgfoid = p.oid
                            ) evt
                        )
                        ELSE NULL
                    END as trigger_events
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                LEFT JOIN pg_description d ON p.oid = d.objoid
                LEFT JOIN pg_depend dep ON dep.objid = p.oid 
                    AND dep.deptype = 'e'
                LEFT JOIN pg_extension ext ON dep.refobjid = ext.oid
                WHERE ext.extname IS NULL
                    AND n.nspname = ANY(:schemas)
                    AND p.proname NOT LIKE 'pg_%'
                    AND p.oid > (
                        SELECT oid 
                        FROM pg_proc 
                        WHERE proname = 'current_database' 
                        LIMIT 1
                    )
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM pg_depend d2
                        JOIN pg_extension e2 ON d2.refobjid = e2.oid
                        WHERE d2.objid = p.oid
                    )
                    AND p.pronamespace > (
                        SELECT oid 
                        FROM pg_namespace 
                        WHERE nspname = 'pg_catalog'
                    )
            )
            SELECT * FROM function_info
            ORDER BY schema, name;
        """

        # Helper functions for processing function metadata
        def get_volatility(volatility_char: str) -> str:
            return {"i": "IMMUTABLE", "s": "STABLE", "v": "VOLATILE"}.get(
                volatility_char, "VOLATILE"
            )

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

        def parse_parameters(args_str: str) -> List[FunctionParameter]:
            if not args_str:
                return []

            parameters = []
            for arg in args_str.split(", "):
                parts = arg.split()

                # Handle different parameter formats
                if parts and parts[0].upper() in ("IN", "OUT", "INOUT", "VARIADIC"):
                    # Procedure format: "IN param_name param_type"
                    mode = parts[0].upper()
                    param_name = parts[1] if len(parts) > 1 else ""
                    param_type = " ".join(parts[2:]) if len(parts) > 2 else ""
                else:
                    # Function format: "param_name param_type"
                    mode = "IN"  # Default mode
                    param_name = parts[0] if parts else ""
                    param_type = " ".join(parts[1:]) if len(parts) > 1 else ""

                parameters.append(
                    FunctionParameter(name=param_name, type=param_type, mode=mode)
                )

            return parameters

        # Execute query
        with next(self.db_client.get_db()) as db:
            result = db.execute(text(query), {"schemas": self.include_schemas})

            for row in result:
                fn_type = determine_function_type(row)
                parameters = parse_parameters(row.arguments)

                metadata = FunctionMetadata(
                    schema=row.schema,
                    name=row.name,
                    return_type=row.return_type if row.return_type else "void",
                    parameters=parameters,
                    type=fn_type,
                    object_type=ObjectType(row.object_type),
                    is_strict=row.is_strict,
                    description=row.description,
                )

                # Categorize based on object_type
                key = f"{row.schema}.{row.name}"

                if row.object_type == "trigger":
                    trigger_cache[key] = metadata
                    log.success(
                        f"Loaded trigger: {color_palette['schema'](row.schema)}.{color_palette['trigger'](row.name)}"
                    )
                elif row.object_type == "procedure":
                    procedure_cache[key] = metadata
                    log.success(
                        f"Loaded procedure: {color_palette['schema'](row.schema)}.{color_palette['procedure'](row.name)}"
                    )
                elif row.object_type == "function":
                    function_cache[key] = metadata
                    log.success(
                        f"Loaded function: {color_palette['schema'](row.schema)}.{color_palette['function'](row.name)}"
                    )

        return function_cache, procedure_cache, trigger_cache

    def _get_sample_data(
        self, schema: str, table_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get a sample row from the table for type inference."""
        try:
            with next(self.db_client.get_db()) as db:
                query = f"SELECT * FROM {schema}.{table_name} LIMIT 1"
                result = db.execute(text(query)).first()
                if result:
                    return {key: value for key, value in result._mapping.items()}
        except Exception as e:
            log.debug(f"Could not get sample data for {schema}.{table_name}: {str(e)}")
        return None

    def log_metadata_stats(self):
        """Print metadata statistics in a table format."""
        stats = {}

        for schema in sorted(self.include_schemas):
            # Count objects by schema
            tables = len(
                [
                    t
                    for t_key, t in self.table_cache.items()
                    if t_key.split(".")[0] == schema
                ]
            )
            views = len(
                [
                    v
                    for v_key, v in self.view_cache.items()
                    if v_key.split(".")[0] == schema
                ]
            )
            enums = len(
                [e for e_key, e in self.enum_cache.items() if e.schema == schema]
            )
            functions = len(
                [f for f_key, f in self.fn_cache.items() if f.schema == schema]
            )
            procedures = len(
                [p for p_key, p in self.proc_cache.items() if p.schema == schema]
            )
            triggers = len(
                [t for t_key, t in self.trig_cache.items() if t.schema == schema]
            )

            # Store counts
            stats[schema] = {
                "tables": tables,
                "views": views,
                "enums": enums,
                "functions": functions,
                "procedures": procedures,
                "triggers": triggers,
            }

        # Build table data for display
        headers = [
            "Schema",
            "Tables",
            "Views",
            "Enums",
            "Fn's",
            "Proc's",
            "Trig's",
            "Total",
        ]
        rows = []

        # Initialize totals
        totals = {
            key: 0
            for key in [
                "tables",
                "views",
                "enums",
                "functions",
                "procedures",
                "triggers",
            ]
        }

        # Add rows for each schema
        for schema, counts in stats.items():
            # Calculate schema total
            schema_total = sum(counts.values())

            # Format row with appropriate colors
            row = [
                color_palette["schema"](schema),
                color_palette["table"](str(counts["tables"])),
                color_palette["view"](str(counts["views"])),
                color_palette["enum"](str(counts["enums"])),
                color_palette["function"](str(counts["functions"])),
                color_palette["procedure"](str(counts["procedures"])),
                color_palette["trigger"](str(counts["triggers"])),
                # color_palette["total"](str(schema_total)),
            ]
            rows.append(row)

            # Update totals
            for key in totals:
                totals[key] += counts[key]

        # Calculate grand total
        grand_total = sum(totals.values())

        # Add totals row
        totals_row = [
            # color_palette["total"]("TOTAL"),
            color_palette["table"](str(totals["tables"])),
            color_palette["view"](str(totals["views"])),
            color_palette["enum"](str(totals["enums"])),
            color_palette["function"](str(totals["functions"])),
            color_palette["procedure"](str(totals["procedures"])),
            color_palette["trigger"](str(totals["triggers"])),
            # color_palette["total"](str(grand_total)),
        ]
        rows.append(totals_row)

        # Print table
        log.section("ModelManager Statistics")
        # log.table(headers, rows)
