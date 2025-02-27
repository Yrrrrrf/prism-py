# src/forge/api/metadata.py
"""
Database metadata API endpoints generation.

This module provides utilities for creating FastAPI routes that expose
database structure information (schemas, tables, views, functions, etc.)
"""

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException

from forge.core.logging import log, color_palette
from forge.db.models import ModelManager

# Import the API response models and conversion functions from types.py
from forge.common.types import (
    ApiColumnReference, 
    ApiColumnMetadata,
    ApiTableMetadata, 
    ApiEnumMetadata,
    ApiFunctionMetadata, 
    ApiTriggerMetadata,
    ApiSchemaMetadata, 
    ApiTriggerEvent,
    to_api_function_parameter,
    to_api_function_metadata
)


# ===== Helper Functions =====
def build_column_metadata(column: Any) -> ApiColumnMetadata:
    """Convert a SQLAlchemy column to ColumnMetadata response model."""
    # Extract foreign key reference if any
    reference = None
    if column.foreign_keys:
        fk = next(iter(column.foreign_keys))
        reference = ApiColumnReference(
            schema=fk.column.table.schema,
            table=fk.column.table.name,
            column=fk.column.name,
        )

    # Create column metadata with appropriate flags
    return ApiColumnMetadata(
        name=column.name,
        type=str(column.type),
        nullable=column.nullable,
        is_primary_key=True if column.primary_key else None,
        is_enum=True if hasattr(column.type, "enums") else None,
        references=reference,
    )


def build_table_metadata(table: Any, schema: str) -> ApiTableMetadata:
    """Convert a SQLAlchemy table to TableMetadata response model."""
    return ApiTableMetadata(
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
            "/schemas", response_model=List[ApiSchemaMetadata], tags=["Metadata"]
        )
        async def get_schemas() -> List[ApiSchemaMetadata]:
            """Get all database schemas with their structure."""
            schemas = []
            for schema_name in self.model_manager.include_schemas:
                # Create schema metadata with all its components
                schema_data = ApiSchemaMetadata(name=schema_name)

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
                        schema_data.enums[enum_key] = ApiEnumMetadata(
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
        self, schema: ApiSchemaMetadata, schema_name: str
    ) -> None:
        """Add functions to a schema metadata object."""
        for fn_key, fn_metadata in self.model_manager.fn_cache.items():
            fn_schema, fn_name = fn_key.split(".")
            if fn_schema == schema_name:
                schema.functions[fn_name] = ApiFunctionMetadata(
                    name=fn_metadata.name,
                    schema=fn_metadata.schema,
                    type=str(fn_metadata.type),
                    object_type=str(fn_metadata.object_type),
                    description=fn_metadata.description,
                    parameters=[
                        to_api_function_parameter(p) for p in fn_metadata.parameters
                    ],
                    return_type=fn_metadata.return_type,
                    is_strict=fn_metadata.is_strict,
                )

    def _add_procedures_to_schema(
        self, schema: ApiSchemaMetadata, schema_name: str
    ) -> None:
        """Add procedures to a schema metadata object."""
        for proc_key, proc_metadata in self.model_manager.proc_cache.items():
            proc_schema, proc_name = proc_key.split(".")
            if proc_schema == schema_name:
                schema.procedures[proc_name] = ApiFunctionMetadata(
                    name=proc_metadata.name,
                    schema=proc_metadata.schema,
                    type=str(proc_metadata.type),
                    object_type=str(proc_metadata.object_type),
                    description=proc_metadata.description,
                    parameters=[
                        to_api_function_parameter(p) for p in proc_metadata.parameters
                    ],
                    return_type=proc_metadata.return_type,
                    is_strict=proc_metadata.is_strict,
                )

    def _add_triggers_to_schema(self, schema: ApiSchemaMetadata, schema_name: str) -> None:
        """Add triggers to a schema metadata object."""
        for trig_key, trig_metadata in self.model_manager.trig_cache.items():
            trig_schema, trig_name = trig_key.split(".")
            if trig_schema == schema_name:
                # Create a simplified trigger event if not available
                trigger_event = ApiTriggerEvent(
                    timing="AFTER",
                    events=["UPDATE"],
                    table_schema=schema_name,
                    table_name="",
                )

                schema.triggers[trig_name] = ApiTriggerMetadata(
                    name=trig_metadata.name,
                    schema=trig_metadata.schema,
                    type=str(trig_metadata.type),
                    object_type=str(trig_metadata.object_type),
                    description=trig_metadata.description,
                    parameters=[
                        to_api_function_parameter(p) for p in trig_metadata.parameters
                    ],
                    return_type=trig_metadata.return_type,
                    is_strict=trig_metadata.is_strict,
                    trigger_data=trigger_event,
                )

    def register_tables_route(self) -> None:
        """Register route to get tables for a specific schema."""

        @self.router.get(
            "/{schema}/tables", response_model=List[ApiTableMetadata], tags=["Metadata"]
        )
        async def get_tables(schema: str) -> List[ApiTableMetadata]:
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
            "/{schema}/views", response_model=List[ApiTableMetadata], tags=["Metadata"]
        )
        async def get_views(schema: str) -> List[ApiTableMetadata]:
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
            "/{schema}/enums", response_model=List[ApiEnumMetadata], tags=["Metadata"]
        )
        async def get_enums(schema: str) -> List[ApiEnumMetadata]:
            """Get all enum types for a specific schema."""
            enums = []

            for enum_name, enum_info in self.model_manager.enum_cache.items():
                if enum_info.schema == schema:
                    enums.append(
                        ApiEnumMetadata(
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
            response_model=List[ApiFunctionMetadata],
            tags=["Metadata"],
        )
        async def get_functions(schema: str) -> List[ApiFunctionMetadata]:
            """Get all functions for a specific schema."""
            functions = []

            for fn_key, fn_metadata in self.model_manager.fn_cache.items():
                fn_schema, _ = fn_key.split(".")
                if fn_schema == schema:
                    functions.append(to_api_function_metadata(fn_metadata))

            if not functions:
                raise HTTPException(
                    status_code=404, detail=f"No functions found in schema '{schema}'"
                )

            return functions

    def register_procedures_route(self) -> None:
        """Register route to get procedures for a specific schema."""

        @self.router.get(
            "/{schema}/procedures",
            response_model=List[ApiFunctionMetadata],
            tags=["Metadata"],
        )
        async def get_procedures(schema: str) -> List[ApiFunctionMetadata]:
            """Get all procedures for a specific schema."""
            procedures = []

            for proc_key, proc_metadata in self.model_manager.proc_cache.items():
                proc_schema, _ = proc_key.split(".")
                if proc_schema == schema:
                    procedures.append(to_api_function_metadata(proc_metadata))

            if not procedures:
                raise HTTPException(
                    status_code=404, detail=f"No procedures found in schema '{schema}'"
                )

            return procedures

    def register_triggers_route(self) -> None:
        """Register route to get triggers for a specific schema."""

        @self.router.get(
            "/{schema}/triggers",
            response_model=List[ApiTriggerMetadata],
            tags=["Metadata"],
        )
        async def get_triggers(schema: str) -> List[ApiTriggerMetadata]:
            """Get all triggers for a specific schema."""
            triggers = []

            for trig_key, trig_metadata in self.model_manager.trig_cache.items():
                trig_schema, _ = trig_key.split(".")
                if trig_schema == schema:
                    # Create a simplified trigger event
                    trigger_event = ApiTriggerEvent(
                        timing="AFTER",
                        events=["UPDATE"],
                        table_schema=schema,
                        table_name="",
                    )

                    base_metadata = to_api_function_metadata(trig_metadata)
                    triggers.append(
                        ApiTriggerMetadata(
                            **base_metadata.model_dump(),
                            trigger_data=trigger_event,
                        )
                    )

            if not triggers:
                raise HTTPException(
                    status_code=404, detail=f"No triggers found in schema '{schema}'"
                )

            return triggers
