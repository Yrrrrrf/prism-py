# src/prism/api/metadata.py
"""
Database metadata API endpoints generation.

This module provides utilities for creating FastAPI routes that expose
database structure information (schemas, tables, views, functions, etc.)
"""

from typing import Any, List

from fastapi import APIRouter, HTTPException

# Import the shared console instance from the new ui module
from prism.ui import console
from prism.common.types import (
    ApiColumnMetadata,
    ApiColumnReference,
    ApiEnumMetadata,
    ApiFunctionMetadata,
    ApiSchemaMetadata,
    ApiTableMetadata,
    ApiTriggerEvent,
    ApiTriggerMetadata,
    to_api_function_metadata,
    to_api_function_parameter,
)
from prism.db.models import ModelManager


# ===== Helper Functions =====
def build_column_metadata(column: Any) -> ApiColumnMetadata:
    """Convert a SQLAlchemy column to ColumnMetadata response model."""
    reference = None
    if column.foreign_keys:
        fk = next(iter(column.foreign_keys))
        reference = ApiColumnReference(
            schema=fk.column.table.schema,
            table=fk.column.table.name,
            column=fk.column.name,
        )
    return ApiColumnMetadata(
        name=column.name,
        type=str(column.type),
        nullable=column.nullable,
        is_primary_key=bool(column.primary_key),
        is_enum=hasattr(column.type, "enums"),
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
                schema_data = ApiSchemaMetadata(name=schema_name)

                # Add tables
                for key, (table, _) in self.model_manager.table_cache.items():
                    if key.startswith(f"{schema_name}."):
                        table_name = key.split(".")[1]
                        schema_data.tables[table_name] = build_table_metadata(
                            table, schema_name
                        )

                # Add views
                for key, (view, _) in self.model_manager.view_cache.items():
                    if key.startswith(f"{schema_name}."):
                        view_name = key.split(".")[1]
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
            if fn_key.startswith(f"{schema_name}."):
                fn_name = fn_key.split(".")[1]
                schema.functions[fn_name] = to_api_function_metadata(fn_metadata)

    def _add_procedures_to_schema(
        self, schema: ApiSchemaMetadata, schema_name: str
    ) -> None:
        """Add procedures to a schema metadata object."""
        for proc_key, proc_metadata in self.model_manager.proc_cache.items():
            if proc_key.startswith(f"{schema_name}."):
                proc_name = proc_key.split(".")[1]
                schema.procedures[proc_name] = to_api_function_metadata(proc_metadata)

    def _add_triggers_to_schema(
        self, schema: ApiSchemaMetadata, schema_name: str
    ) -> None:
        """Add triggers to a schema metadata object."""
        for trig_key, trig_metadata in self.model_manager.trig_cache.items():
            if trig_key.startswith(f"{schema_name}."):
                trig_name = trig_key.split(".")[1]
                # This trigger event data is simplified and may need enhancement
                trigger_event = ApiTriggerEvent(
                    timing="UNKNOWN",
                    events=[],
                    table_schema=schema_name,
                    table_name="unknown",
                )
                base_metadata = to_api_function_metadata(trig_metadata)
                schema.triggers[trig_name] = ApiTriggerMetadata(
                    **base_metadata.model_dump(), trigger_data=trigger_event
                )

    def register_tables_route(self) -> None:
        """Register route to get tables for a specific schema."""

        @self.router.get(
            "/{schema}/tables", response_model=List[ApiTableMetadata], tags=["Metadata"]
        )
        async def get_tables(schema: str) -> List[ApiTableMetadata]:
            """Get all tables for a specific schema."""
            tables = [
                build_table_metadata(table_data[0], schema)
                for key, table_data in self.model_manager.table_cache.items()
                if key.startswith(f"{schema}.")
            ]
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
            views = [
                build_table_metadata(view_data[0], schema)
                for key, view_data in self.model_manager.view_cache.items()
                if key.startswith(f"{schema}.")
            ]
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
            enums = [
                ApiEnumMetadata(name=info.name, schema=schema, values=info.values)
                for info in self.model_manager.enum_cache.values()
                if info.schema == schema
            ]
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
            functions = [
                to_api_function_metadata(metadata)
                for metadata in self.model_manager.fn_cache.values()
                if metadata.schema == schema
            ]
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
            procedures = [
                to_api_function_metadata(metadata)
                for metadata in self.model_manager.proc_cache.values()
                if metadata.schema == schema
            ]
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
            for trig_metadata in self.model_manager.trig_cache.values():
                if trig_metadata.schema == schema:
                    trigger_event = ApiTriggerEvent(
                        timing="UNKNOWN",
                        events=[],
                        table_schema=schema,
                        table_name="unknown",
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
