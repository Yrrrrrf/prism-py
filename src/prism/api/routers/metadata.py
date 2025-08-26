# src/prism/api/routers/metadata.py
from typing import List

from fastapi import APIRouter, FastAPI, HTTPException

# Import the new public API models
from prism.api.models import (
    ApiColumnMetadata,
    ApiColumnReference,
    ApiEnumMetadata,
    ApiFunctionMetadata,
    ApiFunctionParameter,
    ApiSchemaMetadata,
    ApiTableMetadata,
)
from prism.cache import CacheManager, SchemaCache

# Import the internal models for type hinting in helpers
from prism.core.models.functions import FunctionMetadata as InternalFunctionMetadata
from prism.core.models.tables import TableMetadata as InternalTableMetadata

# ===== Helper functions to convert internal models to public API models =====


def _build_api_table(internal_table: InternalTableMetadata) -> ApiTableMetadata:
    """Converts an internal TableMetadata to its public API representation."""
    columns = []
    for col in internal_table.columns:
        reference = None
        if col.foreign_key:
            reference = ApiColumnReference(
                schema=col.foreign_key.schema,
                table=col.foreign_key.table,
                column=col.foreign_key.column,
            )
        columns.append(
            ApiColumnMetadata(
                name=col.name,
                type=col.sql_type,
                nullable=col.is_nullable,
                is_pk=col.is_pk,
                # The internal enum_info is a link, but for the API we just need a boolean flag.
                is_enum=bool(col.enum_info),
                references=reference,
            )
        )
    return ApiTableMetadata(
        name=internal_table.name,
        schema=internal_table.schema,
        columns=columns,
    )


def _build_api_function(internal_func: InternalFunctionMetadata) -> ApiFunctionMetadata:
    """Converts an internal FunctionMetadata to its public API representation."""
    return ApiFunctionMetadata(
        name=internal_func.name,
        schema=internal_func.schema,
        return_type=internal_func.return_type or "void",
        parameters=[
            ApiFunctionParameter(name=p.name, type=p.type, mode=p.mode)
            for p in internal_func.parameters
        ],
    )


class MetadataGenerator:
    """Generates metadata routes for database schema inspection."""

    def __init__(self, app: FastAPI, cache_manager: CacheManager):
        self.app = app
        self.cache_manager = cache_manager
        self.router = APIRouter(prefix="/dt", tags=["Metadata"])

    def generate_routes(self):
        """Creates and registers all metadata-related endpoints."""

        # --- Main "Full Map" Endpoint ---
        @self.router.get(
            "/schemas",
            response_model=List[ApiSchemaMetadata],
            summary="Get a full map of all database schemas and their contents",
        )
        def get_full_schemas() -> List[ApiSchemaMetadata]:
            response_list = []
            for schema_name, schema_cache in self.cache_manager.cache.items():
                api_schema = ApiSchemaMetadata(name=schema_name)
                for table in schema_cache.tables:
                    api_schema.tables[table.name] = _build_api_table(table)
                for view in schema_cache.views:
                    api_schema.views[view.name] = _build_api_table(view)
                for enum_name, enum_info in schema_cache.enums.items():
                    api_schema.enums[enum_name] = ApiEnumMetadata(**enum_info.__dict__)
                for func in schema_cache.functions:
                    api_schema.functions[func.name] = _build_api_function(func)
                for proc in schema_cache.procedures:
                    api_schema.procedures[proc.name] = _build_api_function(proc)
                for trig in schema_cache.triggers:
                    api_schema.triggers[trig.name] = _build_api_function(trig)
                response_list.append(api_schema)
            if not response_list:
                raise HTTPException(
                    status_code=404, detail="No schemas found or introspected."
                )
            return response_list

        # --- Helper for Granular Endpoints ---
        def _get_schema_cache_or_404(schema: str) -> SchemaCache:
            schema_cache = self.cache_manager.get_schema(schema)
            if not schema_cache:
                raise HTTPException(
                    status_code=404,
                    detail=f"Schema '{schema}' not found or not introspected.",
                )
            return schema_cache

        # --- Granular Endpoints for Specific Object Types ---

        @self.router.get(
            "/{schema}/tables",
            response_model=List[ApiTableMetadata],
            summary="List all tables in a schema",
        )
        def get_tables(schema: str) -> List[ApiTableMetadata]:
            return [
                _build_api_table(t) for t in _get_schema_cache_or_404(schema).tables
            ]

        @self.router.get(
            "/{schema}/views",
            response_model=List[ApiTableMetadata],
            summary="List all views in a schema",
        )
        def get_views(schema: str) -> List[ApiTableMetadata]:
            return [_build_api_table(v) for v in _get_schema_cache_or_404(schema).views]

        @self.router.get(
            "/{schema}/enums",
            response_model=List[ApiEnumMetadata],
            summary="List all enums in a schema",
        )
        def get_enums(schema: str) -> List[ApiEnumMetadata]:
            return [
                ApiEnumMetadata(**e.__dict__)
                for e in _get_schema_cache_or_404(schema).enums.values()
            ]

        @self.router.get(
            "/{schema}/functions",
            response_model=List[ApiFunctionMetadata],
            summary="List all functions in a schema",
        )
        def get_functions(schema: str) -> List[ApiFunctionMetadata]:
            return [
                _build_api_function(f)
                for f in _get_schema_cache_or_404(schema).functions
            ]

        @self.router.get(
            "/{schema}/procedures",
            response_model=List[ApiFunctionMetadata],
            summary="List all procedures in a schema",
        )
        def get_procedures(schema: str) -> List[ApiFunctionMetadata]:
            return [
                _build_api_function(p)
                for p in _get_schema_cache_or_404(schema).procedures
            ]

        @self.router.get(
            "/{schema}/triggers",
            response_model=List[ApiFunctionMetadata],
            summary="List all trigger functions in a schema",
        )
        def get_triggers(schema: str) -> List[ApiFunctionMetadata]:
            return [
                _build_api_function(t)
                for t in _get_schema_cache_or_404(schema).triggers
            ]

        # Finally, register the router with all its endpoints to the app
        self.app.include_router(self.router)
