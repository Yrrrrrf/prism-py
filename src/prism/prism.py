# src/prism/prism.py
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, FastAPI
from sqlalchemy.engine import Engine

from prism.api.routers.crud import CrudGenerator
from prism.api.routers.functions import (
    FunctionGenerator,
    ProcedureGenerator,
    TriggerGenerator,
)
from prism.api.routers.health import HealthGenerator
from prism.api.routers.metadata import MetadataGenerator
from prism.api.routers.views import ViewGenerator
from prism.cache import CacheManager
from prism.core.introspection.base import IntrospectorABC
from prism.core.introspection.postgres import PostgresIntrospector
from prism.db.client import DbClient
from prism.ui import console, display_route_links, print_welcome


class ApiPrism:
    """Main API generation and management class."""

    def __init__(
        self, db_client: DbClient, app: FastAPI, schemas: Optional[List[str]] = None
    ):
        self.db_client = db_client
        self.app = app
        self.schemas = schemas
        self.introspector = self._get_introspector(db_client.engine)
        self.cache: Optional[CacheManager] = None
        self.start_time = datetime.now(timezone.utc)
        self._introspected = False

    def _get_introspector(self, engine: Engine) -> IntrospectorABC:
        return PostgresIntrospector(engine)

    def _ensure_introspection(self):
        """Runs introspection only if it hasn't been run before."""
        if self._introspected:
            return

        schemas_to_process: List[str]
        if not self.schemas:
            console.print(
                "[dim]No schemas provided. Discovering all user-defined schemas...[/]"
            )
            schemas_to_process = self.introspector.get_schemas()
            if not schemas_to_process:
                raise ValueError("No user-defined schemas found in the database.")
        else:
            schemas_to_process = self.schemas

        self.cache = CacheManager(schemas=schemas_to_process)

        console.rule("[bold cyan]Introspecting Database Schema", style="bold cyan")
        with console.status("[bold green]Analyzing database schema..."):
            for schema in schemas_to_process:
                console.print(f"  Analysing schema: '[bold]{schema}[/]'")

                schema_cache = self.cache.get_schema(schema)
                if not schema_cache:
                    continue

                schema_cache.tables = self.introspector.get_tables(schema=schema)
                for table in schema_cache.tables:
                    console.print(f"\t[dim]table: [blue]{table.name}[/]")

                schema_cache.views = self.introspector.get_views(schema=schema)
                for view in schema_cache.views:
                    console.print(f"\t[dim]view: [green]{view.name}[/]")

                schema_cache.enums = self.introspector.get_enums(schema=schema)
                for enum_name in schema_cache.enums.keys():
                    console.print(f"\t[dim]enum: [magenta]{enum_name}[/]")

                schema_cache.functions = self.introspector.get_functions(schema=schema)
                for func in schema_cache.functions:
                    console.print(f"\t[dim]function: [red]{func.name}[/]")

                schema_cache.procedures = self.introspector.get_procedures(
                    schema=schema
                )
                for proc in schema_cache.procedures:
                    console.print(f"\t[dim]procedure: [yellow]{proc.name}[/]")

                schema_cache.triggers = self.introspector.get_triggers(schema=schema)
                for trig in schema_cache.triggers:
                    console.print(f"\t[dim]trigger: [orange1]{trig.name}[/]")

                console.print()

        console.print("[bold green]✅ Introspection Complete.[/]\n")
        self._introspected = True

    def gen_table_routes(self):
        """Generates and includes CRUD routes for all tables."""
        self._ensure_introspection()
        if not self.cache:
            return
        console.rule("[bold blue]Generating Table Routes", style="bold blue")

        routers_for_this_call: Dict[str, APIRouter] = {}
        generated_count = 0

        for schema in self.cache.schemas:
            schema_cache = self.cache.get_schema(schema)
            if not schema_cache or not schema_cache.tables:
                continue

            router = routers_for_this_call.setdefault(
                schema, APIRouter(prefix=f"/{schema}", tags=[schema.upper()])
            )

            for table_meta in schema_cache.tables:
                gen = CrudGenerator(
                    table_metadata=table_meta,
                    db_dependency=self.db_client.get_db,
                    router=router,
                    engine=self.db_client.engine,
                )
                gen.generate_routes()
                generated_count += 1

        for router in routers_for_this_call.values():
            self.app.include_router(router)

        console.print(f"[bold blue]Generated routes for {generated_count} tables.[/]\n")

    def gen_view_routes(self):
        """Generates and includes read-only routes for all database views."""
        self._ensure_introspection()
        if not self.cache:
            return
        console.rule("[bold green]Generating View Routes", style="bold green")

        routers_for_this_call: Dict[str, APIRouter] = {}
        generated_count = 0

        for schema in self.cache.schemas:
            schema_cache = self.cache.get_schema(schema)
            if not schema_cache or not schema_cache.views:
                continue

            router = routers_for_this_call.setdefault(
                schema, APIRouter(prefix=f"/{schema}", tags=[schema.upper()])
            )

            for view_meta in schema_cache.views:
                gen = ViewGenerator(
                    view_metadata=view_meta,
                    db_dependency=self.db_client.get_db,
                    router=router,
                )
                gen.generate_routes()
                generated_count += 1

        for router in routers_for_this_call.values():
            self.app.include_router(router)

        console.print(f"[bold green]Generated routes for {generated_count} views.[/]\n")

    def gen_fn_routes(self):
        """Generates and includes POST routes for database functions."""
        self._ensure_introspection()
        if not self.cache:
            return
        console.rule("[bold red]Generating Function Routes", style="bold red")

        routers_for_this_call: Dict[str, APIRouter] = {}
        generated_count = 0

        for schema in self.cache.schemas:
            schema_cache = self.cache.get_schema(schema)
            if not schema_cache or not schema_cache.functions:
                continue

            router = routers_for_this_call.setdefault(
                schema, APIRouter(prefix=f"/{schema}", tags=[schema.upper()])
            )

            for func_meta in schema_cache.functions:
                gen = FunctionGenerator(
                    metadata=func_meta,
                    db_dependency=self.db_client.get_db,
                    router=router,
                )
                gen.generate_routes()
                generated_count += 1

        for router in routers_for_this_call.values():
            self.app.include_router(router)

        console.print(
            f"[bold red]Generated routes for {generated_count} functions.[/]\n"
        )

    def gen_proc_routes(self):
        """Generates and includes POST routes for database procedures."""
        self._ensure_introspection()
        if not self.cache:
            return
        console.rule("[bold magenta]Generating Procedure Routes", style="bold magenta")

        routers_for_this_call: Dict[str, APIRouter] = {}
        generated_count = 0

        for schema in self.cache.schemas:
            schema_cache = self.cache.get_schema(schema)
            if not schema_cache or not schema_cache.procedures:
                continue

            router = routers_for_this_call.setdefault(
                schema, APIRouter(prefix=f"/{schema}", tags=[schema.upper()])
            )

            for proc_meta in schema_cache.procedures:
                gen = ProcedureGenerator(
                    metadata=proc_meta,
                    db_dependency=self.db_client.get_db,
                    router=router,
                )
                gen.generate_routes()
                generated_count += 1

        for router in routers_for_this_call.values():
            self.app.include_router(router)

        console.print(
            f"[bold magenta]Generated routes for {generated_count} procedures.[/]\n"
        )

    def gen_trig_routes(self):
        """Analyzes triggers but does not generate routes."""
        self._ensure_introspection()
        if not self.cache:
            return
        console.rule("[bold orange1]Analyzing Triggers", style="bold orange1")

        for schema in self.cache.schemas:
            schema_cache = self.cache.get_schema(schema)
            if not schema_cache or not schema_cache.triggers:
                console.print(
                    f"  [dim]No triggers found in schema '[bold]{schema}[/]'[/dim]"
                )
                continue
            for trig_meta in schema_cache.triggers:
                gen = TriggerGenerator(metadata=trig_meta)
                gen.generate_routes()
        console.print()

    def gen_metadata_routes(self):
        """Generates and includes metadata routes."""
        self._ensure_introspection()
        if not self.cache:
            return
        console.rule("[bold cyan]Generating Metadata Routes", style="bold cyan")
        gen = MetadataGenerator(app=self.app, cache_manager=self.cache)
        gen.generate_routes()
        display_route_links(
            db_client=self.db_client,
            title="Metadata API",
            tag="Metadata",
            endpoints={
                "Get all schemas": ("/dt/schemas", "get_full_schemas", "GET"),
                "Get schema tables": ("/dt/{schema}/tables", "get_tables", "GET"),
                "Get schema views": ("/dt/{schema}/views", "get_views", "GET"),
                "Get schema functions": (
                    "/dt/{schema}/functions",
                    "get_functions",
                    "GET",
                ),
                "Get schema procedures": (
                    "/dt/{schema}/procedures",
                    "get_procedures",
                    "GET",
                ),
                "Get schema triggers": ("/dt/{schema}/triggers", "get_triggers", "GET"),
                "Get schema enums": ("/dt/{schema}/enums", "get_enums", "GET"),
            },
        )

    def gen_health_routes(self):
        """Generates and includes health routes."""
        self._ensure_introspection()
        if not self.cache:
            return
        console.rule("[bold pink1]Generating Health Routes", style="bold pink1")
        gen = HealthGenerator(app=self.app, prism_instance=self)
        gen.generate_routes()
        display_route_links(
            db_client=self.db_client,
            title="Health API",
            tag="Health",
            endpoints={
                "Full service status": ("/health/", "get_health", "GET"),
                "Simple ping check": ("/health/ping", "ping", "GET"),
                "Clear and reload cache": (
                    "/health/clear-cache",
                    "clear_cache",
                    "POST",
                ),
            },
        )

    def gen_all_routes(self):
        """Introspects the database and generates all available API routes."""
        self._ensure_introspection()
        # if not self.cache:
        #     return

        # # Add all discovered enums to the OpenAPI schema components
        # if not self.app.openapi_components:
        #     self.app.openapi_components = {}
        # if "schemas" not in self.app.openapi_components:
        #     self.app.openapi_components["schemas"] = {}

        # for schema_cache in self.cache.cache.values():
        #     for enum_info in schema_cache.enums.values():
        #         self.app.openapi_components["schemas"][enum_info.name] = {
        #             "title": enum_info.name,
        #             "enum": enum_info.values,
        #             "type": "string",
        #             "description": f"An enumeration for {enum_info.name} in schema {enum_info.schema}",
        #         }

        self.gen_metadata_routes()
        self.gen_health_routes()
        self.gen_table_routes()
        self.gen_view_routes()
        self.gen_fn_routes()
        self.gen_proc_routes()
        self.gen_trig_routes()

        console.print("[bold green]✅ API Generation Complete.[/]\n")

    def print_welcome_message(self, host: str, port: int):
        print_welcome(
            project_name=f"Prism-py: {self.db_client.engine.url.database}",
            version="0.1.0-refactored",
            host=host,
            port=port,
        )


# # * Additional utility methods

# # def add_custom_route(
# #     self,
# #     path: str,
# #     endpoint: Callable,
# #     methods: List[str] = ["GET"],
# #     tags: List[str] = None,
# #     summary: str = None,
# #     description: str = None,
# #     response_model: Type = None
# # ) -> None:
# #     """
# #     Add a custom route to the API.

# #     Allows adding custom endpoints beyond the automatically generated ones.

# #     Args:
# #         path: Route path
# #         endpoint: Endpoint handler function
# #         methods: HTTP methods to support
# #         tags: OpenAPI tags
# #         summary: Route summary
# #         description: Route description
# #         response_model: Pydantic response model
# #     """
# #     # Create router for custom routes if needed
# #     if "custom" not in self.routers:
# #         self.routers["custom"] = APIRouter(tags=["Custom"])

# #     # Add route
# #     self.routers["custom"].add_api_route(
# #         path=path,
# #         endpoint=endpoint,
# #         methods=methods,
# #         tags=tags,
# #         summary=summary,
# #         description=description,
# #         response_model=response_model
# #     )

# #     # Ensure router is registered
# #     if "custom" not in [r.prefix for r in self.app.routes]:
# #         self.app.include_router(self.routers["custom"])

# #     log.success(f"Added custom route: {path}")
