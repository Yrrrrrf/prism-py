"""
Database metadata API endpoints generation.

This module provides utilities for creating FastAPI routes that expose
database structure information (schemas, tables, views, functions, etc.)
"""

from typing import Dict, List, Optional, Callable, Any, Type, Union, TYPE_CHECKING
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from forge.core.logging import log, color_palette
from forge.common.types import FunctionMetadata, FunctionParameter

# Use TYPE_CHECKING for model manager reference
from forge.db.models import ModelManager

# ===== Response Models =====


class ColumnReference(BaseModel):
    """Reference to another database column (for foreign keys)."""

    schema_name: str = Field(
        alias="schema"
    )  # Use Field alias to maintain compatibility
    table: str
    column: str


class ColumnMetadata(BaseModel):
    """Column metadata for table or view."""

    name: str
    type: str
    nullable: bool
    is_primary_key: Optional[bool] = Field(default=None, alias="is_pk")
    is_enum: Optional[bool] = None
    references: Optional[ColumnReference] = None


class EntityMetadata(BaseModel):
    """Base class for database entity metadata."""

    name: str
    schema_name: str = Field(alias="schema")  # Use Field alias for compatibility


class TableMetadata(EntityMetadata):
    """Table structure metadata."""

    columns: List[ColumnMetadata] = []


class EnumValue(BaseModel):
    """Enum value information."""

    name: str
    value: str


class EnumMetadata(EntityMetadata):
    """Enum type metadata."""

    values: List[str] = []


class FunctionParameter(BaseModel):
    """Function parameter metadata."""

    name: str
    type: str
    mode: str = "IN"  # IN, OUT, INOUT, VARIADIC
    has_default: bool = False
    default_value: Optional[str] = None


class ReturnColumn(BaseModel):
    """Return column for table-returning functions."""

    name: str
    type: str


class FunctionMetadata(EntityMetadata):
    """Database function metadata."""

    type: str  # scalar, table, set, etc.
    object_type: str  # function, procedure, trigger
    description: Optional[str] = None
    parameters: List[FunctionParameter] = []
    return_type: Optional[str] = None
    return_columns: Optional[List[ReturnColumn]] = None
    is_strict: bool = False


class TriggerEvent(BaseModel):
    """Trigger event information."""

    timing: str  # BEFORE, AFTER, INSTEAD OF
    events: List[str]  # INSERT, UPDATE, DELETE, TRUNCATE
    table_schema: str
    table_name: str


class TriggerMetadata(FunctionMetadata):
    """Trigger metadata extending function metadata."""

    trigger_data: TriggerEvent


class SchemaMetadata(BaseModel):
    """Complete schema metadata including all database objects."""

    name: str
    tables: Dict[str, TableMetadata] = {}
    views: Dict[str, TableMetadata] = {}
    enums: Dict[str, EnumMetadata] = {}
    functions: Dict[str, FunctionMetadata] = {}
    procedures: Dict[str, FunctionMetadata] = {}
    triggers: Dict[str, TriggerMetadata] = {}


# ===== Helper Functions =====


def build_column_metadata(column: Any) -> ColumnMetadata:
    """Convert a SQLAlchemy column to ColumnMetadata response model."""
    # Extract foreign key reference if any
    reference = None
    if column.foreign_keys:
        fk = next(iter(column.foreign_keys))
        reference = ColumnReference(
            schema=fk.column.table.schema,
            table=fk.column.table.name,
            column=fk.column.name,
        )

    # Create column metadata with appropriate flags
    return ColumnMetadata(
        name=column.name,
        type=str(column.type),
        nullable=column.nullable,
        is_primary_key=True if column.primary_key else None,
        is_enum=True if hasattr(column.type, "enums") else None,
        references=reference,
    )


def build_table_metadata(table: Any, schema: str) -> TableMetadata:
    """Convert a SQLAlchemy table to TableMetadata response model."""
    return TableMetadata(
        name=table.name,
        schema=schema,
        columns=[build_column_metadata(col) for col in table.columns],
    )


# ===== Main Router Class =====


class MetadataRouter:
    """Metadata route generator for database structure endpoints."""

    def __init__(self, router: APIRouter, model_manager: ModelManager):
        """
        Initialize the metadata router.

        Args:
            router: FastAPI router to attach routes to
            model_manager: ModelManager containing database metadata
        """
        self.router = router
        self.model_manager = model_manager
        self.prefix = "/dt"  # Default prefix for metadata routes

    def register_all_routes(self) -> None:
        """Register all metadata routes."""
        log.info(f"Registering metadata routes with prefix {self.prefix}")

        # Register routes
        self.register_schemas_route()
        self.register_tables_route()
        self.register_views_route()
        self.register_enums_route()
        self.register_functions_route()
        self.register_procedures_route()
        self.register_triggers_route()

    def register_schemas_route(self) -> None:
        """Register route to get all schemas with their contents."""

        @self.router.get(
            "/schemas", response_model=List[SchemaMetadata], tags=["Metadata"]
        )
        async def get_schemas() -> List[SchemaMetadata]:
            """Get all database schemas with their structure."""
            schemas = []
            for schema_name in self.model_manager.include_schemas:
                # Create schema metadata with all its components
                schema_data = SchemaMetadata(name=schema_name)

                # Add tables
                for key, table_data in self.model_manager.table_cache.items():
                    table_schema, table_name = key.split(".")
                    if table_schema == schema_name:
                        table, _ = table_data
                        schema_data.tables[table_name] = build_table_metadata(
                            table, schema_name
                        )

                # Add views
                for key, view_data in self.model_manager.view_cache.items():
                    view_schema, view_name = key.split(".")
                    if view_schema == schema_name:
                        view, _ = view_data
                        schema_data.views[view_name] = build_table_metadata(
                            view, schema_name
                        )

                # Add enums
                for enum_key, enum_info in self.model_manager.enum_cache.items():
                    if enum_info.schema == schema_name:
                        schema_data.enums[enum_key] = EnumMetadata(
                            name=enum_info.name,
                            schema=schema_name,
                            values=enum_info.values,
                        )

                # Add functions, procedures, and triggers
                self._add_functions_to_schema(schema_data, schema_name)
                self._add_procedures_to_schema(schema_data, schema_name)
                self._add_triggers_to_schema(schema_data, schema_name)

                schemas.append(schema_data)

            if not schemas:
                raise HTTPException(status_code=404, detail="No schemas found")

            return schemas

    def _add_functions_to_schema(
        self, schema: SchemaMetadata, schema_name: str
    ) -> None:
        """Add functions to a schema metadata object."""
        for fn_key, fn_metadata in self.model_manager.fn_cache.items():
            fn_schema, fn_name = fn_key.split(".")
            if fn_schema == schema_name:
                schema.functions[fn_name] = FunctionMetadata(
                    name=fn_metadata.name,
                    schema=fn_metadata.schema,
                    type=fn_metadata.type,
                    object_type=fn_metadata.object_type,
                    description=fn_metadata.description,
                    parameters=[
                        FunctionParameter(
                            name=p.name,
                            type=p.type,
                            mode=p.mode,
                            has_default=p.has_default,
                            default_value=p.default_value,
                        )
                        for p in fn_metadata.parameters
                    ],
                    return_type=fn_metadata.return_type,
                    is_strict=fn_metadata.is_strict,
                )

    def _add_procedures_to_schema(
        self, schema: SchemaMetadata, schema_name: str
    ) -> None:
        """Add procedures to a schema metadata object."""
        for proc_key, proc_metadata in self.model_manager.proc_cache.items():
            proc_schema, proc_name = proc_key.split(".")
            if proc_schema == schema_name:
                schema.procedures[proc_name] = FunctionMetadata(
                    name=proc_metadata.name,
                    schema=proc_metadata.schema,
                    type=proc_metadata.type,
                    object_type=proc_metadata.object_type,
                    description=proc_metadata.description,
                    parameters=[
                        FunctionParameter(
                            name=p.name,
                            type=p.type,
                            mode=p.mode,
                            has_default=p.has_default,
                            default_value=p.default_value,
                        )
                        for p in proc_metadata.parameters
                    ],
                    return_type=proc_metadata.return_type,
                    is_strict=proc_metadata.is_strict,
                )

    def _add_triggers_to_schema(self, schema: SchemaMetadata, schema_name: str) -> None:
        """Add triggers to a schema metadata object."""
        for trig_key, trig_metadata in self.model_manager.trig_cache.items():
            trig_schema, trig_name = trig_key.split(".")
            if trig_schema == schema_name:
                # Create a simplified trigger event if not available
                trigger_event = TriggerEvent(
                    timing="AFTER",
                    events=["UPDATE"],
                    table_schema=schema_name,
                    table_name="",
                )

                schema.triggers[trig_name] = TriggerMetadata(
                    name=trig_metadata.name,
                    schema=trig_metadata.schema,
                    type=trig_metadata.type,
                    object_type=trig_metadata.object_type,
                    description=trig_metadata.description,
                    parameters=[
                        FunctionParameter(
                            name=p.name,
                            type=p.type,
                            mode=p.mode,
                            has_default=p.has_default,
                            default_value=p.default_value,
                        )
                        for p in trig_metadata.parameters
                    ],
                    return_type=trig_metadata.return_type,
                    is_strict=trig_metadata.is_strict,
                    trigger_data=trigger_event,
                )

    def register_tables_route(self) -> None:
        """Register route to get tables for a specific schema."""

        @self.router.get(
            "/{schema}/tables", response_model=List[TableMetadata], tags=["Metadata"]
        )
        async def get_tables(schema: str) -> List[TableMetadata]:
            """Get all tables for a specific schema."""
            tables = []

            for table_key, table_data in self.model_manager.table_cache.items():
                table_schema, _ = table_key.split(".")
                if table_schema == schema:
                    table, _ = table_data
                    tables.append(build_table_metadata(table, schema))

            if not tables:
                raise HTTPException(
                    status_code=404, detail=f"No tables found in schema '{schema}'"
                )

            return tables

    def register_views_route(self) -> None:
        """Register route to get views for a specific schema."""

        @self.router.get(
            "/{schema}/views", response_model=List[TableMetadata], tags=["Metadata"]
        )
        async def get_views(schema: str) -> List[TableMetadata]:
            """Get all views for a specific schema."""
            views = []

            for view_key, view_data in self.model_manager.view_cache.items():
                view_schema, _ = view_key.split(".")
                if view_schema == schema:
                    view, _ = view_data
                    views.append(build_table_metadata(view, schema))

            if not views:
                raise HTTPException(
                    status_code=404, detail=f"No views found in schema '{schema}'"
                )

            return views

    def register_enums_route(self) -> None:
        """Register route to get enums for a specific schema."""

        @self.router.get(
            "/{schema}/enums", response_model=List[EnumMetadata], tags=["Metadata"]
        )
        async def get_enums(schema: str) -> List[EnumMetadata]:
            """Get all enum types for a specific schema."""
            enums = []

            for enum_name, enum_info in self.model_manager.enum_cache.items():
                if enum_info.schema == schema:
                    enums.append(
                        EnumMetadata(
                            name=enum_info.name, schema=schema, values=enum_info.values
                        )
                    )

            if not enums:
                raise HTTPException(
                    status_code=404, detail=f"No enums found in schema '{schema}'"
                )

            return enums

    def register_functions_route(self) -> None:
        """Register route to get functions for a specific schema."""

        @self.router.get(
            "/{schema}/functions",
            response_model=List[FunctionMetadata],
            tags=["Metadata"],
        )
        async def get_functions(schema: str) -> List[FunctionMetadata]:
            """Get all functions for a specific schema."""
            functions = []

            for fn_key, fn_metadata in self.model_manager.fn_cache.items():
                fn_schema, _ = fn_key.split(".")
                if fn_schema == schema:
                    functions.append(
                        FunctionMetadata(
                            name=fn_metadata.name,
                            schema=fn_metadata.schema,
                            type=fn_metadata.type,
                            object_type=fn_metadata.object_type,
                            description=fn_metadata.description,
                            parameters=[
                                FunctionParameter(
                                    name=p.name,
                                    type=p.type,
                                    mode=p.mode,
                                    has_default=p.has_default,
                                    default_value=p.default_value,
                                )
                                for p in fn_metadata.parameters
                            ],
                            return_type=fn_metadata.return_type,
                            is_strict=fn_metadata.is_strict,
                        )
                    )

            if not functions:
                raise HTTPException(
                    status_code=404, detail=f"No functions found in schema '{schema}'"
                )

            return functions

    def register_procedures_route(self) -> None:
        """Register route to get procedures for a specific schema."""

        @self.router.get(
            "/{schema}/procedures",
            response_model=List[FunctionMetadata],
            tags=["Metadata"],
        )
        async def get_procedures(schema: str) -> List[FunctionMetadata]:
            """Get all procedures for a specific schema."""
            procedures = []

            for proc_key, proc_metadata in self.model_manager.proc_cache.items():
                proc_schema, _ = proc_key.split(".")
                if proc_schema == schema:
                    procedures.append(
                        FunctionMetadata(
                            name=proc_metadata.name,
                            schema=proc_metadata.schema,
                            type=proc_metadata.type,
                            object_type=proc_metadata.object_type,
                            description=proc_metadata.description,
                            parameters=[
                                FunctionParameter(
                                    name=p.name,
                                    type=p.type,
                                    mode=p.mode,
                                    has_default=p.has_default,
                                    default_value=p.default_value,
                                )
                                for p in proc_metadata.parameters
                            ],
                            return_type=proc_metadata.return_type,
                            is_strict=proc_metadata.is_strict,
                        )
                    )

            if not procedures:
                raise HTTPException(
                    status_code=404, detail=f"No procedures found in schema '{schema}'"
                )

            return procedures

    def register_triggers_route(self) -> None:
        """Register route to get triggers for a specific schema."""

        @self.router.get(
            "/{schema}/triggers",
            response_model=List[TriggerMetadata],
            tags=["Metadata"],
        )
        async def get_triggers(schema: str) -> List[TriggerMetadata]:
            """Get all triggers for a specific schema."""
            triggers = []

            for trig_key, trig_metadata in self.model_manager.trig_cache.items():
                trig_schema, _ = trig_key.split(".")
                if trig_schema == schema:
                    # Create a simplified trigger event
                    trigger_event = TriggerEvent(
                        timing="AFTER",
                        events=["UPDATE"],
                        table_schema=schema,
                        table_name="",
                    )

                    triggers.append(
                        TriggerMetadata(
                            name=trig_metadata.name,
                            schema=trig_metadata.schema,
                            type=trig_metadata.type,
                            object_type=trig_metadata.object_type,
                            description=trig_metadata.description,
                            parameters=[
                                FunctionParameter(
                                    name=p.name,
                                    type=p.type,
                                    mode=p.mode,
                                    has_default=p.has_default,
                                    default_value=p.default_value,
                                )
                                for p in trig_metadata.parameters
                            ],
                            return_type=trig_metadata.return_type,
                            is_strict=trig_metadata.is_strict,
                            trigger_data=trigger_event,
                        )
                    )

            if not triggers:
                raise HTTPException(
                    status_code=404, detail=f"No triggers found in schema '{schema}'"
                )

            return triggers


# ===== Function to Generate Metadata Routes =====


# def generate_metadata_routes(router: APIRouter, model_manager: ModelManager) -> None:
#     """
#     Generate all metadata routes and attach them to the provided router.

#     Args:
#         router: FastAPI router to attach routes to
#         model_manager: ModelManager containing database metadata
#     """
#     metadata_router = MetadataRouter(router, model_manager)
#     metadata_router.register_all_routes()
