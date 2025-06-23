# src/prism/api/routers/metadata.py
from typing import Dict, List

from fastapi import APIRouter, FastAPI, HTTPException

from prism.core.models.enums import EnumInfo
from prism.core.models.functions import FunctionMetadata
from prism.core.models.tables import TableMetadata


class MetadataGenerator:
    """Generates metadata routes for database schema inspection."""

    def __init__(self, app: FastAPI, db_metadata: Dict[str, Dict]):
        self.app = app
        self.db_meta = db_metadata
        self.router = APIRouter(prefix="/dt", tags=["Metadata"])

    def generate_routes(self):
        """Creates and registers all metadata-related endpoints."""

        @self.router.get("/schemas", summary="List all database schemas and structure")
        def get_schemas() -> dict[str, dict]:
            """Returns a summary of all introspected schemas and the number of items in each."""
            summary = {}
            for schema, meta in self.db_meta.items():
                summary[schema] = {
                    "tables": len([t for t in meta.get("tables", []) if not t.is_view]),
                    "views": len([t for t in meta.get("tables", []) if t.is_view]),
                    "functions": len(meta.get("functions", [])),
                    "procedures": len(meta.get("procedures", [])),
                    "triggers": len(meta.get("triggers", [])),
                    "enums": len(meta.get("enums", {})),
                }
            return summary

        def _get_schema_or_404(schema: str):
            if schema not in self.db_meta:
                raise HTTPException(
                    status_code=404,
                    detail=f"Schema '{schema}' not found or not introspected.",
                )
            return self.db_meta[schema]

        @self.router.get(
            "/{schema}/tables",
            response_model=List[TableMetadata],
            summary="List all tables in a schema",
        )
        def get_tables(schema: str) -> List[TableMetadata]:
            meta = _get_schema_or_404(schema)
            return [t for t in meta.get("tables", []) if not t.is_view]

        @self.router.get(
            "/{schema}/views",
            response_model=List[TableMetadata],
            summary="List all views in a schema",
        )
        def get_views(schema: str) -> List[TableMetadata]:
            meta = _get_schema_or_404(schema)
            return [t for t in meta.get("tables", []) if t.is_view]

        @self.router.get(
            "/{schema}/functions",
            response_model=List[FunctionMetadata],
            summary="List all functions in a schema",
        )
        def get_functions(schema: str) -> List[FunctionMetadata]:
            meta = _get_schema_or_404(schema)
            return meta.get("functions", [])

        @self.router.get(
            "/{schema}/procedures",
            response_model=List[FunctionMetadata],
            summary="List all procedures in a schema",
        )
        def get_procedures(schema: str) -> List[FunctionMetadata]:
            meta = _get_schema_or_404(schema)
            return meta.get("procedures", [])

        @self.router.get(
            "/{schema}/triggers",
            response_model=List[FunctionMetadata],
            summary="List all triggers in a schema",
        )
        def get_triggers(schema: str) -> List[FunctionMetadata]:
            meta = _get_schema_or_404(schema)
            return meta.get("triggers", [])

        @self.router.get(
            "/{schema}/enums",
            response_model=List[EnumInfo],
            summary="List all enums in a schema",
        )
        def get_enums(schema: str) -> List[EnumInfo]:
            meta = _get_schema_or_404(schema)
            return list(meta.get("enums", {}).values())

        # Register the completed router with the main FastAPI application
        self.app.include_router(self.router)
