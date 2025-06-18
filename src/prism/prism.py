"""Main Prism API generator."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import Field, create_model
from sqlalchemy import text

from prism.api.metadata import MetadataRouter
from prism.common.types import (
    ArrayType,
    FunctionMetadata,
    FunctionType,
    PrismBaseModel,
    get_eq_type,
)
from prism.config import PrismConfig
from prism.db.client import DbClient
from prism.db.models import ModelManager
from prism.ui import (
    console,
    display_function_structure,
    display_route_links,
    display_table_structure,
    print_welcome,
)

# Optional: For internal logging that is not part of the user-facing CLI output
# This is different from the console output and can be configured with RichHandler
# in the main application script if needed.
log = logging.getLogger(__name__)


class ApiPrism:
    """Main API generation and management class."""

    def __init__(self, config: PrismConfig, app: Optional[FastAPI] = None):
        """Initialize the API Prism instance."""
        self.config = config
        self.app = app or FastAPI()
        self.routers: Dict[str, APIRouter] = {}
        self.start_time = datetime.now()
        self._initialize_app()

    def _initialize_app(self) -> None:
        """Initialize FastAPI app configuration."""
        # Configure FastAPI app with our settings
        self.app.title = self.config.project_name
        self.app.version = self.config.version
        self.app.description = self.config.description

        if self.config.author:
            self.app.contact = {"name": self.config.author, "email": self.config.email}

        if self.config.license_info:
            self.app.license_info = self.config.license_info

        # Add CORS middleware by default
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def print_welcome(self, db_client: DbClient) -> None:
        """Print welcome message with app information."""
        print()
        print_welcome(
            project_name=self.config.project_name,
            version=self.config.version,
            host=db_client.config.host,
            port=8000
        )
        print()

    def gen_table_routes(
        self, model_manager: ModelManager, enhanced_filtering: bool = True
    ) -> None:
        """
        Generate CRUD routes for all tables.

        Args:
            model_manager: The model manager containing database metadata
            enhanced_filtering: Whether to enable enhanced filtering (sorting, pagination)
        """
        console.rule("[bold blue]Generating Table Routes", style="bold blue")

        from prism.api.crud import CrudGenerator

        # Initialize routers for each schema
        for schema in model_manager.include_schemas:
            if schema not in self.routers:
                self.routers[schema] = APIRouter(
                    prefix=f"/{schema}", tags=[schema.upper()]
                )

        # Generate routes for each table
        for table_key, table_data in model_manager.table_cache.items():
            schema, table_name = table_key.split(".")
            console.print(f"Generating CRUD for: [cyan]{schema}.[bold]{table_name}[/]")

            # Call the new UI function instead of the old private method
            display_table_structure(table_data[0])

            table, (pydantic_model, sqlalchemy_model) = table_data

            # Create and use CRUD generator with enhanced filtering option
            generator = CrudGenerator(
                table=table,
                pydantic_model=pydantic_model,
                sqlalchemy_model=sqlalchemy_model,
                router=self.routers[schema],
                db_dependency=model_manager.db_client.get_db,
                schema=schema,
                enhanced_filtering=enhanced_filtering,
            )
            generator.generate_routes()

        # Register routers with the app
        for schema in model_manager.include_schemas:
            if schema in self.routers:
                self.app.include_router(self.routers[schema])

        console.print(
            f"[dim bold blue]Generated table routes for {len(model_manager.table_cache)} tables\n"
        )

    def gen_view_routes(self, model_manager: ModelManager) -> None:
        """Generate routes for all views."""
        console.rule(
            "[bold light_green]Generating View Routes", style="bold light_green"
        )

        from prism.api.views import ViewGenerator

        # Initialize view routers for each schema
        for schema in model_manager.include_schemas:
            router_key = f"{schema}_views"
            if router_key not in self.routers:
                self.routers[router_key] = APIRouter(
                    prefix=f"/{schema}", tags=[f"{schema.upper()} Views"]
                )

        # Inside the for loop where views are processed
        for view_key, view_data in model_manager.view_cache.items():
            schema, view_name = view_key.split(".")
            console.print(
                f"Generating view route for: [cyan]{schema}[/].[green]{view_name}[/]"
            )

            # Call the new UI function
            display_table_structure(view_data[0])

            table, (query_model, response_model) = view_data
            router = self.routers[f"{schema}_views"]

            # Create and use View generator
            generator = ViewGenerator(
                table=table,
                query_model=query_model,
                response_model=response_model,
                router=router,
                db_dependency=model_manager.db_client.get_db,
                schema=schema,
            )
            generator.generate_routes()

        # Register all view routers with the app
        for schema in model_manager.include_schemas:
            router_key = f"{schema}_views"
            if router_key in self.routers:
                self.app.include_router(self.routers[router_key])

        console.print(
            f"[dim bold green]Generated view routes for {len(model_manager.view_cache)} views\n"
        )

    def gen_fn_routes(self, model_manager: ModelManager) -> None:
        """
        Generate routes for all functions, procedures, and triggers.
        """
        console.rule(
            "[bold red]Generating Function & Procedure Routes", style="bold red"
        )

        # Initialize function routers for each schema
        for schema in model_manager.include_schemas:
            router_key = f"{schema}_fn"
            if router_key not in self.routers:
                self.routers[router_key] = APIRouter(
                    prefix=f"/{schema}", tags=[f"{schema.upper()} Functions"]
                )

        # Process regular functions
        self._generate_function_routes(model_manager, model_manager.fn_cache, "fn")

        # Process procedures
        self._generate_function_routes(model_manager, model_manager.proc_cache, "proc")

        # Register all function routers with the app
        for schema in model_manager.include_schemas:
            router_key = f"{schema}_fn"
            if router_key in self.routers:
                self.app.include_router(self.routers[router_key])

        console.print(
            f"[dim bold red]Generated function routes for {len(model_manager.fn_cache)} functions "
            f"and {len(model_manager.proc_cache)} procedures\n"
        )

    def gen_metadata_routes(self, model_manager: ModelManager) -> None:
        """
        Generate metadata routes for database schema inspection.
        """
        console.rule("[bold cyan]Generating Metadata Routes", style="bold cyan")

        tag = "Metadata"
        router = APIRouter(prefix="/dt", tags=[tag])

        metadata_router = MetadataRouter(router, model_manager)
        metadata_router.register_all_routes()
        self.app.include_router(router)

        display_route_links(
            db_client=model_manager.db_client,
            title="Metadata API",
            tag=tag,
            endpoints={
                # "Description": ("path_with_prefix", "handler_function_name", "METHOD")
                "Get schema structure": ("/dt/schemas", "get_schemas", "GET"),
                "Get schema tables": ("/dt/{schema}/tables", "get_tables", "GET"),
                "Get schema views": ("/dt/{schema}/views", "get_views", "GET"),
                "Get schema enums": ("/dt/{schema}/enums", "get_enums", "GET"),
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
                "Get schema triggers": (
                    "/dt/{schema}/triggers",
                    "get_triggers",
                    "GET",
                ),
            },
        )
        console.print("\n[dim bold cyan]Generated metadata routes\n")

    def gen_health_routes(self, model_manager: ModelManager) -> None:
        """
        Generate health check routes for API monitoring.
        """
        console.rule("[bold green]Generating Health Routes", style="bold green")

        from prism.api.health import HealthGenerator

        tag = "Health"
        router = APIRouter(prefix="/health", tags=[tag])

        generator = HealthGenerator(
            router=router,
            model_manager=model_manager,
            version=self.config.version,
            start_time=self.start_time,
        )
        generator.generate_routes()
        self.app.include_router(router)

        display_route_links(
            db_client=model_manager.db_client,
            title="Health API",
            tag=tag,
            endpoints={
                # "Description": ("/path", "handler_function_name", "METHOD")
                "Full status check": ("/health", "health_check", "GET"),
                "Simple ping": ("/health/ping", "ping", "GET"),
                "Cache status": ("/health/cache", "cache_status", "GET"),
                "Clear cache": ("/health/clear-cache", "clear_cache", "POST"),
            },
        )
        console.print("\n[dim bold green]Generated health routes\n")

    def generate_all_routes(self, model_manager: ModelManager) -> None:
        """
        Generate all routes for the API.

        Convenience method to generate all route types in the recommended order.
        """
        self.gen_metadata_routes(model_manager)
        self.gen_health_routes(model_manager)
        self.gen_table_routes(model_manager)
        self.gen_view_routes(model_manager)
        self.gen_fn_routes(model_manager)

    def _generate_function_routes(
        self,
        model_manager: ModelManager,
        function_cache: Dict[str, FunctionMetadata],
        route_type: str,
    ) -> None:
        """
        Generate routes for a specific type of database function.
        """
        # Note: The `InputModel` variable below might be flagged by some linters
        # because it's defined inside a loop. This is necessary for creating
        # unique Pydantic models per function and is functionally correct for FastAPI.

        def create_procedure_handler(
            schema: str, fn_name: str, fn_metadata: FunctionMetadata
        ):
            """Create a handler for procedures that correctly captures the current scope."""

            async def execute_procedure(
                params: Any,  # Using Any to satisfy linters, FastAPI will use the correct InputModel
                db=Depends(model_manager.db_client.get_db),
            ):
                param_list = [f":{p}" for p in params.model_fields.keys()]
                query = f"CALL {schema}.{fn_name}({', '.join(param_list)})"

                log.debug(f"Executing procedure query: {query}")
                console.print(
                    "[bold yellow]WARN:[/] Procedure route generator is not fully tested."
                )

                db.execute(text(query), params.model_dump())
                return {
                    "status": "success",
                    "message": f"Procedure {fn_name} executed successfully",
                }

            execute_procedure.__name__ = f"execute_procedure_{schema}_{fn_name}"
            return execute_procedure

        def create_function_handler(
            schema: str,
            fn_name: str,
            fn_metadata: FunctionMetadata,
            is_set: bool,
            OutputModel: Any,
        ):
            """Create a handler for functions that correctly captures the current scope."""

            async def execute_function(
                params: Any,  # Using Any to satisfy linters, FastAPI will use the correct InputModel
                db=Depends(model_manager.db_client.get_db),
            ):
                try:
                    param_list = [f":{p}" for p in params.model_fields.keys()]
                    param_values = params.model_dump()
                    query = f"SELECT * FROM {schema}.{fn_name}({', '.join(param_list)})"

                    console.print(f"[dim]Executing query: {query}[/]")
                    console.print(f"[dim]Parameters: {param_values}[/]")

                    result = db.execute(text(query), param_values)

                    if fn_metadata.type == FunctionType.SCALAR:
                        record = result.fetchone()
                        if record is None:
                            return OutputModel.model_validate({"result": None})
                        if len(record._mapping) == 1:
                            value = list(record._mapping.values())[0]
                            return OutputModel.model_validate({"result": value})
                        else:
                            return OutputModel.model_validate(dict(record._mapping))
                    else:
                        records = result.fetchall()
                        if not records:
                            return (
                                []
                                if is_set
                                else OutputModel.model_validate(
                                    {
                                        field: None
                                        for field in OutputModel.model_fields.keys()
                                    }
                                )
                            )
                        if len(records) > 1 or is_set:
                            return [
                                OutputModel.model_validate(dict(r._mapping))
                                for r in records
                            ]
                        return OutputModel.model_validate(dict(records[0]._mapping))
                except Exception as e:
                    log.error(
                        f"Error executing function {schema}.{fn_name}: {e}",
                        exc_info=True,
                    )
                    raise HTTPException(
                        status_code=500, detail=f"Error executing function: {str(e)}"
                    )

            execute_function.__name__ = f"execute_function_{schema}_{fn_name}"
            return execute_function

        for fn_key, fn_metadata in function_cache.items():
            schema, fn_name = fn_key.split(".")
            router_key = f"{schema}_fn"
            if router_key not in self.routers:
                continue
            router = self.routers[router_key]

            fn_color = "magenta" if route_type == "fn" else "yellow"
            console.print(
                f"Generating {route_type} route for: [cyan]{schema}[/].[{fn_color}]{fn_name}[/]"
            )
            display_function_structure(fn_metadata)

            input_fields = {}
            for param in fn_metadata.parameters:
                field_type = get_eq_type(param.type)
                if isinstance(field_type, ArrayType):
                    field_type = List[field_type.item_type]
                input_fields[param.name] = (
                    field_type if not param.has_default else Optional[field_type],
                    Field(default=param.default_value if param.has_default else ...),
                )
            InputModel = create_model(
                f"{route_type.capitalize()}_{schema}_{fn_name}_Input",
                __base__=PrismBaseModel,
                **input_fields,
            )

            if route_type == "proc":
                procedure_handler = create_procedure_handler(
                    schema, fn_name, fn_metadata
                )
                router.add_api_route(
                    path=f"/proc/{fn_name}",
                    endpoint=procedure_handler,
                    methods=["POST"],
                    response_model=None,
                    status_code=200,
                    # Pass the dynamically created model here for FastAPI to use
                    dependencies=[Depends(lambda: InputModel)],
                    summary=f"Execute {fn_name} procedure",
                    description=fn_metadata.description
                    or f"Execute the {fn_name} procedure",
                )
            else:
                is_set = fn_metadata.type in (
                    FunctionType.TABLE,
                    FunctionType.SET_RETURNING,
                )
                output_fields = {}
                if is_set or "TABLE" in (fn_metadata.return_type or ""):
                    output_fields = self._parse_table_return_type(
                        fn_metadata.return_type or ""
                    )
                else:
                    output_type = get_eq_type(fn_metadata.return_type or "void")
                    if isinstance(output_type, ArrayType):
                        output_type = List[output_type.item_type]
                    output_fields = {"result": (output_type, ...)}
                if not output_fields:
                    output_fields = {"result": (Any, ...)}
                OutputModel = create_model(
                    f"{route_type.capitalize()}_{schema}_{fn_name}_Output",
                    __base__=PrismBaseModel,
                    **output_fields,
                )
                function_handler = create_function_handler(
                    schema, fn_name, fn_metadata, is_set, OutputModel
                )
                router.add_api_route(
                    path=f"/fn/{fn_name}",
                    endpoint=function_handler,
                    methods=["POST"],
                    response_model=Union[List[OutputModel], OutputModel],
                    # Pass the dynamically created model here for FastAPI to use
                    dependencies=[Depends(lambda: InputModel)],
                    summary=f"Execute {fn_name} function",
                    description=fn_metadata.description
                    or f"Execute the {fn_name} function",
                )

    def _parse_table_return_type(self, return_type: str) -> Dict[str, Any]:
        """
        Parse TABLE and SETOF return types into field definitions.
        """
        fields = {}
        if not return_type:
            return {"result": (str, ...)}
        if not ("TABLE" in return_type or "SETOF" in return_type):
            field_type = get_eq_type(return_type)
            return {"result": (field_type, ...)}
        if "TABLE" in return_type:
            try:
                columns_str = return_type.replace("TABLE", "").strip("()").strip()
                if not columns_str:
                    return {"result": (str, ...)}
                columns = [col.strip() for col in columns_str.split(",")]
                for column in columns:
                    if " " not in column:
                        console.print(
                            f"[yellow]WARN:[/] Invalid column definition in TABLE type: {column}"
                        )
                        continue
                    name, type_str = column.split(" ", 1)
                    field_type = get_eq_type(type_str)
                    if isinstance(field_type, ArrayType):
                        field_type = List[field_type.item_type]
                    fields[name] = (field_type, ...)
            except Exception as e:
                console.print(
                    f"[red]ERROR:[/] Error parsing TABLE return type '{return_type}': {e}"
                )
                return {"result": (str, ...)}
        elif "SETOF" in return_type:
            try:
                type_str = return_type.replace("SETOF", "").strip()
                if "." in type_str:
                    return {"result": (str, ...)}
                field_type = get_eq_type(type_str)
                fields["result"] = (field_type, ...)
            except Exception as e:
                console.print(
                    f"[red]ERROR:[/] Error parsing SETOF return type '{return_type}': {e}"
                )
                return {"result": (str, ...)}
        if not fields:
            return {"result": (str, ...)}
        return fields


# * Additional utility methods

# def add_custom_route(
#     self,
#     path: str,
#     endpoint: Callable,
#     methods: List[str] = ["GET"],
#     tags: List[str] = None,
#     summary: str = None,
#     description: str = None,
#     response_model: Type = None
# ) -> None:
#     """
#     Add a custom route to the API.

#     Allows adding custom endpoints beyond the automatically generated ones.

#     Args:
#         path: Route path
#         endpoint: Endpoint handler function
#         methods: HTTP methods to support
#         tags: OpenAPI tags
#         summary: Route summary
#         description: Route description
#         response_model: Pydantic response model
#     """
#     # Create router for custom routes if needed
#     if "custom" not in self.routers:
#         self.routers["custom"] = APIRouter(tags=["Custom"])

#     # Add route
#     self.routers["custom"].add_api_route(
#         path=path,
#         endpoint=endpoint,
#         methods=methods,
#         tags=tags,
#         summary=summary,
#         description=description,
#         response_model=response_model
#     )

#     # Ensure router is registered
#     if "custom" not in [r.prefix for r in self.app.routes]:
#         self.app.include_router(self.routers["custom"])

#     log.success(f"Added custom route: {path}")


# todo: Check if this is needed...
# def configure_error_handlers(self) -> None:
#     """
#     Configure global error handlers for the API.

#     Sets up custom exception handlers for common error types.
#     """
#     @self.app.exception_handler(HTTPException)
#     async def http_exception_handler(request, exc):
#         return JSONResponse(
#             status_code=exc.status_code,
#             content={
#                 "error": True,
#                 "message": exc.detail,
#                 "status_code": exc.status_code
#             }
#         )


#     @self.app.exception_handler(Exception)
#     async def general_exception_handler(request, exc):
#         # Log the error
#         log.error(f"Unhandled exception: {str(exc)}")

#         return JSONResponse(
#             status_code=500,
#             content={
#                 "error": True,
#                 "message": "Internal server error",
#                 "detail": str(exc) if self.config.debug_mode else None,
#                 "status_code": 500
#             }
#         )

#     log.success("Configured global error handlers")
