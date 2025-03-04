"""Main Prism API generator."""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Column, Table
from sqlalchemy import Enum as SQLAlchemyEnum

from prism.api.metadata import MetadataRouter
from prism.common.types import FunctionMetadata, FunctionType, JSONBType, get_eq_type
from prism.core.config import PrismConfig
from prism.core.logging import (
    blue,
    bold,
    color_palette,
    dim,
    green,
    log,
    red,
    violet,
    yellow,
)
from prism.db.client import DbClient
from prism.db.models import ModelManager


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
        db_client.test_connection()
        docs_url = f"http://{db_client.config.host}:8000/docs"
        log.info(
            f"{self.config.project_name} initialized (version {self.config.version})"
        )
        log.info(f"API Documentation: {docs_url}")

    def _print_table_structure(self, table: Table) -> None:
        """Print detailed table structure with columns and enums."""

        def get_column_flags(column: Column) -> List[str]:
            """Get formatted flags for a column."""
            flags = []
            if column.primary_key:
                flags.append(f"{green('PK')}")
            if column.foreign_keys:
                ref_table = next(iter(column.foreign_keys)).column.table
                flags.append(
                    f"{blue(f'FK → {ref_table.schema}.{bold(ref_table.name)}')}"
                )
            return flags

        def get_base_type(type_: Any) -> str:
            """Extract base type from Optional types."""
            type_str = str(type_)  # Get the string representation

            if "typing.Optional" in type_str:
                return re.search(r"\[(.*)\]", type_str).group(1).split(".")[-1]

            match isinstance(type_, type):  # Handle non-Optional types
                case True:
                    return type_.__name__  # Return class name if it's a type
                case False:
                    return str(type_)  # Return string representation otherwise

        # Print columns
        for column in table.columns:
            flags_str = " ".join(get_column_flags(column))
            py_type = get_eq_type(str(column.type))
            nullable = "" if column.nullable else "*"

            # Determine type string and values based on column type
            match column.type:
                case _ if isinstance(column.type, SQLAlchemyEnum):
                    type_str = f"{yellow(f'Enum({column.type.name})')}"
                    values = f"{dim(str(column.type.enums))}"
                case _:
                    values = ""
                    if isinstance(py_type, JSONBType):
                        type_str = violet("JSONB")
                    else:
                        type_str = violet(get_base_type(py_type))

            log.simple(
                f"\t\t{column.name:<24} {red(f'{nullable:<2}')}{dim(str(column.type)[:20]):<32} "
                f"{type_str:<16} {flags_str} {values if values else ''}"
            )
        log.simple("")

    def gen_table_routes(
        self, model_manager: ModelManager, enhanced_filtering: bool = True
    ) -> None:
        """
        Generate CRUD routes for all tables.

        Args:
            model_manager: The model manager containing database metadata
            enhanced_filtering: Whether to enable enhanced filtering (sorting, pagination)
        """
        log.section("Generating Table Routes")

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
            log.info(
                f"Generating CRUD for: {color_palette['schema'](schema)}.{color_palette['table'](table_name)}"
            )

            self._print_table_structure(table_data[0])  # table_data[0] is the table

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

        log.warn(f"Generated table routes for {len(model_manager.table_cache)} tables")

    def gen_view_routes(self, model_manager: ModelManager) -> None:
        """Generate routes for all views."""
        log.section("Generating View Routes")

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
            log.info(
                f"Generating view route for: {color_palette['schema'](schema)}.{color_palette['view'](view_name)}"
            )

            # Add this line to log the view structure when it's processed
            self._print_table_structure(view_data[0])  # view_data[0] is the view

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

        log.warn(f"Generated view routes for {len(model_manager.view_cache)} views")

    def gen_metadata_routes(self, model_manager: ModelManager) -> None:
        """
        Generate metadata routes for database schema inspection.

        Creates endpoints to explore database structure including:
        - Schemas
        - Tables
        - Views
        - Functions
        - Enums
        """
        log.section("Generating Metadata Routes")

        # Create metadata router
        router = APIRouter(prefix="/dt", tags=["Metadata"])

        # Create and configure metadata router
        metadata_router = MetadataRouter(router, model_manager)
        metadata_router.register_all_routes()

        # Register the router with the app
        self.app.include_router(router)

        log.warn("Generated metadata routes")

    def gen_health_routes(self, model_manager: ModelManager) -> None:
        """
        Generate health check routes for API monitoring.

        Creates endpoints to check API health and status:
        - Health check
        - Database connectivity
        - Cache status
        - Ping endpoint
        """
        log.section("Generating Health Routes")

        from prism.api.health import HealthGenerator

        # Create health router
        router = APIRouter(prefix="/health", tags=["Health"])

        # Create and use health generator
        generator = HealthGenerator(
            router=router,
            model_manager=model_manager,
            version=self.config.version,
            start_time=self.start_time,
        )
        generator.generate_routes()

        # Register the router with the app
        self.app.include_router(router)

        log.warn("Generated health routes")

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

    def gen_fn_routes(self, model_manager: ModelManager) -> None:
        """
        Generate routes for all functions, procedures, and triggers.

        Creates endpoints to execute database functions with proper parameter handling.
        """
        log.section("Generating Function Routes")

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

        log.warn(
            f"Generated function routes for {len(model_manager.fn_cache)} functions "
            f"and {len(model_manager.proc_cache)} procedures"
        )

    def _generate_function_routes(
        self,
        model_manager: ModelManager,
        function_cache: Dict[str, FunctionMetadata],
        route_type: str,
    ) -> None:
        """
        Generate routes for a specific type of database function.

        Args:
            model_manager: The model manager containing database metadata
            function_cache: Dictionary of function metadata
            route_type: Type of route to generate ('fn' or 'proc')
        """
        # Import necessary types at the function level to ensure they're available throughout
        from typing import Any, Optional

        from pydantic import Field, create_model
        from sqlalchemy import text

        from prism.common.types import ArrayType, PrismBaseModel, get_eq_type

        # Create function factories to avoid closure issues
        def create_procedure_handler(
            schema: str, fn_name: str, fn_metadata: FunctionMetadata
        ):
            """Create a handler for procedures that correctly captures the current schema and function name."""

            async def execute_procedure(
                params: InputModel, db=Depends(model_manager.db_client.get_db)
            ):
                # Build parameter list
                param_list = [f":{p}" for p in params.model_fields.keys()]

                # Create query using the correct schema and function name
                query = f"CALL {schema}.{fn_name}({', '.join(param_list)})"

                # Log query for debugging
                log.debug(f"Executing query: {query}")
                log.warn(
                    red("CHECK THIS!!! I MEAN, THE PROC GENERATOR IS NOT TESTED YET!!!")
                )
                log.warn(
                    red("CHECK THIS!!! I MEAN, THE PROC GENERATOR IS NOT TESTED YET!!!")
                )
                log.warn(
                    red("CHECK THIS!!! I MEAN, THE PROC GENERATOR IS NOT TESTED YET!!!")
                )

                # Execute procedure
                db.execute(text(query), params.model_dump())

                return {
                    "status": "success",
                    "message": f"Procedure {fn_name} executed successfully",
                }

            # Set a unique operation_id to avoid FastAPI route conflicts
            execute_procedure.__name__ = f"execute_procedure_{schema}_{fn_name}"
            return execute_procedure

        def create_function_handler(
            schema: str,
            fn_name: str,
            fn_metadata: FunctionMetadata,
            is_set: bool,
            OutputModel,
        ):
            """Create a handler for functions that correctly captures the current schema and function name."""

            async def execute_function(
                params: InputModel, db=Depends(model_manager.db_client.get_db)
            ):
                try:
                    # Build parameter list for the query
                    param_list = [f":{p}" for p in params.model_fields.keys()]
                    param_values = params.model_dump()

                    # Create query using the correct schema and function name
                    query = f"SELECT * FROM {schema}.{fn_name}({', '.join(param_list)})"

                    # Log the query and parameters for debugging
                    log.trace(f"Executing query: {query}")
                    log.trace(f"Parameters: {param_values}")

                    # Execute function
                    result = db.execute(text(query), param_values)

                    # Special handling for scalar functions that return a single value
                    if fn_metadata.type == FunctionType.SCALAR:
                        # For scalar functions, we always return a single record
                        record = result.fetchone()

                        if record is None:
                            # Handle null result
                            return OutputModel.model_validate({"result": None})

                        # Check if it's a single column result
                        if len(record._mapping) == 1:
                            # It's a scalar result with a single column
                            value = list(record._mapping.values())[0]
                            return OutputModel.model_validate({"result": value})
                        else:
                            # Multiple columns returned for the single record
                            return OutputModel.model_validate(dict(record._mapping))

                    # For table and set returning functions
                    else:
                        records = result.fetchall()

                        # If no records, return an empty instance of OutputModel
                        # to maintain correct typing
                        if not records:
                            if is_set:
                                # For set returning functions, return empty list
                                return []
                            else:
                                # For non-set functions, return null values
                                default_values = {
                                    field: None
                                    for field in OutputModel.model_fields.keys()
                                }
                                return OutputModel.model_validate(default_values)

                        # For multiple records
                        if len(records) > 1 or is_set:
                            return [
                                OutputModel.model_validate(dict(r._mapping))
                                for r in records
                            ]

                        # For single record (non-set functions)
                        return OutputModel.model_validate(dict(records[0]._mapping))

                except Exception as e:
                    # Provide detailed error information
                    log.error(f"Error executing function {schema}.{fn_name}: {str(e)}")
                    raise HTTPException(
                        status_code=500, detail=f"Error executing function: {str(e)}"
                    )

            # Set a unique operation_id to avoid FastAPI route conflicts
            execute_function.__name__ = f"execute_function_{schema}_{fn_name}"
            return execute_function

        # Generate routes for each function
        # Inside the _generate_function_routes method where functions are processed
        for fn_key, fn_metadata in function_cache.items():
            schema, fn_name = fn_key.split(".")
            router_key = f"{schema}_fn"

            if router_key not in self.routers:
                continue

            router = self.routers[router_key]

            # Log function route generation
            log.info(
                f"Generating {route_type} route for: "
                f"{color_palette['schema'](schema)}."
                f"{color_palette['function' if route_type == 'fn' else 'procedure'](fn_name)}"
            )

            self._print_function_structure(fn_metadata)

            # Create input model for parameters
            input_fields = {}
            for param in fn_metadata.parameters:
                # Get parameter type
                field_type = get_eq_type(param.type)

                # Handle array types
                if isinstance(field_type, ArrayType):
                    field_type = List[field_type.item_type]

                # Create field
                input_fields[param.name] = (
                    field_type if not param.has_default else Optional[field_type],
                    Field(default=param.default_value if param.has_default else ...),
                )

            # Create unique input model for this function
            InputModel = create_model(
                f"{route_type.capitalize()}_{schema}_{fn_name}_Input",
                __base__=PrismBaseModel,
                **input_fields,
            )

            if route_type == "proc":
                # Generate procedure route with proper scope handling
                procedure_handler = create_procedure_handler(
                    schema, fn_name, fn_metadata
                )

                router.add_api_route(
                    path=f"/proc/{fn_name}",
                    endpoint=procedure_handler,
                    methods=["POST"],
                    summary=f"Execute {fn_name} procedure",
                    description=fn_metadata.description
                    or f"Execute the {fn_name} procedure",
                )
            else:
                # Determine function return type
                is_set = fn_metadata.type in (
                    FunctionType.TABLE,
                    FunctionType.SET_RETURNING,
                )

                # Determine what kind of output we should have
                output_fields = {}

                # Handle different return types
                if is_set or "TABLE" in (fn_metadata.return_type or ""):
                    # Parse TABLE return type
                    output_fields = self._parse_table_return_type(
                        fn_metadata.return_type or ""
                    )
                else:
                    # Handle scalar return
                    # For scalar functions, we always expect a single value result
                    output_type = get_eq_type(fn_metadata.return_type or "void")

                    # Handle array types
                    if isinstance(output_type, ArrayType):
                        output_type = List[output_type.item_type]

                    # Create result field (scalar functions return single value)
                    output_fields = {"result": (output_type, ...)}

                # If no fields could be determined, use a generic field
                if not output_fields:
                    output_fields = {"result": (Any, ...)}

                # Create unique output model for this function
                OutputModel = create_model(
                    f"{route_type.capitalize()}_{schema}_{fn_name}_Output",
                    __base__=PrismBaseModel,
                    **output_fields,
                )

                # Create the function handler with proper scope handling
                function_handler = create_function_handler(
                    schema, fn_name, fn_metadata, is_set, OutputModel
                )

                # Add the route
                router.add_api_route(
                    path=f"/fn/{fn_name}",
                    endpoint=function_handler,
                    methods=["POST"],
                    response_model=Union[List[OutputModel], OutputModel],
                    summary=f"Execute {fn_name} function",
                    description=fn_metadata.description
                    or f"Execute the {fn_name} function",
                )

    def _parse_table_return_type(self, return_type: str) -> Dict[str, Any]:
        """
        Parse TABLE and SETOF return types into field definitions.

        Args:
            return_type: Function return type string

        Returns:
            Dictionary of field definitions
        """
        from prism.common.types import ArrayType, get_eq_type

        fields = {}

        # Handle empty or None return type
        if not return_type:
            return {"result": (str, ...)}

        # # Debug information
        # log.debug(f"Parsing return type: {return_type}")

        # Handle scalar return types that might be mislabeled
        if not ("TABLE" in return_type or "SETOF" in return_type):
            # Simple scalar return type
            field_type = get_eq_type(return_type)
            return {"result": (field_type, ...)}

        # Parse TABLE type
        if "TABLE" in return_type:
            try:
                # Strip 'TABLE' and parentheses
                columns_str = return_type.replace("TABLE", "").strip("()").strip()

                # Handle empty TABLE definition
                if not columns_str:
                    return {"result": (str, ...)}

                columns = [col.strip() for col in columns_str.split(",")]

                for column in columns:
                    # Ensure the column definition has a space between name and type
                    if " " not in column:
                        log.warning(
                            f"Invalid column definition in TABLE type: {column}"
                        )
                        continue

                    name, type_str = column.split(" ", 1)
                    field_type = get_eq_type(type_str)

                    # Handle ArrayType in table columns
                    if isinstance(field_type, ArrayType):
                        field_type = List[field_type.item_type]

                    fields[name] = (field_type, ...)
            except Exception as e:
                log.error(f"Error parsing TABLE return type '{return_type}': {str(e)}")
                # Fallback to generic field
                return {"result": (str, ...)}

        # Handle SETOF type
        elif "SETOF" in return_type:
            try:
                # Extract the type after SETOF
                type_str = return_type.replace("SETOF", "").strip()

                # If it's a composite type (not handled yet)
                if "." in type_str:  # Like "schema.type_name"
                    return {"result": (str, ...)}

                # Simple type
                field_type = get_eq_type(type_str)
                fields["result"] = (field_type, ...)
            except Exception as e:
                log.error(f"Error parsing SETOF return type '{return_type}': {str(e)}")
                # Fallback to generic field
                return {"result": (str, ...)}

        # If we couldn't parse anything meaningful, return a generic field
        if not fields:
            return {"result": (str, ...)}

        return fields

    def _print_function_structure(self, fn_metadata: FunctionMetadata) -> None:
        """Print detailed function structure with parameters and return type."""
        # Format and print the return type
        return_type = fn_metadata.return_type or "void"
        fn_type = str(fn_metadata.type).split(".")[-1]  # Get just the enum name part

        # Create a type description with color formatting
        if "TABLE" in return_type:
            type_display = violet("TABLE")
            if "(" in return_type:
                # For table functions with column definitions
                columns_part = return_type.replace("TABLE", "").strip("() ")
                type_display = f"{type_display}{dim(f'({columns_part})')}"
        elif "SETOF" in return_type:
            base_type = return_type.replace("SETOF", "").strip()
            type_display = f"{violet('SETOF')} {violet(base_type)}"
        else:
            type_display = violet(return_type)

        log.simple(f"\t-> {type_display} {yellow(f'({fn_type})')}")

        if fn_metadata.description:
            log.simple(f"{dim(fn_metadata.description)}")

        # Print parameters section header if there are any
        if fn_metadata.parameters:
            # Print each parameter with consistent formatting
            for param in fn_metadata.parameters:
                # Format parameter mode
                mode_str = ""

                match param.mode:
                    case "IN":
                        mode_str = dim("IN")
                    case "OUT":
                        mode_str = green("OUT")
                    case "INOUT":
                        mode_str = yellow("INOUT")
                    case "VARIADIC":
                        mode_str = blue("VARIADIC")

                # Format default value if present
                default_str = ""
                if param.has_default:
                    default_value = (
                        str(param.default_value)
                        if param.default_value is not None
                        else "NULL"
                    )
                    default_str = dim(f" DEFAULT {default_value}")

                # Print parameter with consistent spacing/formatting as table columns
                log.simple(
                    f"\t\t{param.name:<22} {red('  ')}{dim(param.type):<28} "
                    f"{violet(mode_str):<8}{default_str}"
                )
            log.simple("")

        # Print additional metadata if available
        if fn_metadata.is_strict:
            log.simple(f"\t\t{'Strict:':<24} {yellow('TRUE')}")


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
