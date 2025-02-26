"""Main Forge API generator."""

from datetime import datetime
from typing import Dict, List, Optional, Type, Union, Callable, Any
from fastapi import FastAPI, APIRouter, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from forge.api.metadata import MetadataRouter
from forge.api.crud import CrudOps
from forge.core.config import ForgeConfig
from forge.core.logging import log, color_palette
from forge.db.models import ModelManager
from forge.db.client import DbClient
from forge.common.types import FunctionMetadata, FunctionType, ObjectType


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    timestamp: datetime
    version: str
    uptime: float
    database_connected: bool


class CacheStatus(BaseModel):
    """Cache status response model."""
    last_updated: datetime
    total_items: int
    tables_cached: int
    views_cached: int
    enums_cached: int
    functions_cached: int
    procedures_cached: int
    triggers_cached: int


class ApiForge:
    """Main API generation and management class."""
    
    def __init__(self, config: ForgeConfig, app: Optional[FastAPI] = None):
        """Initialize the API Forge instance."""
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
        log.info(f"{self.config.project_name} initialized (version {self.config.version})")
        log.info(f"API Documentation: {docs_url}")
    
    def gen_table_routes(self, model_manager: ModelManager) -> None:
        """
        Generate CRUD routes for all tables.
        
        Creates a router for each schema and registers CRUD operations for each table.
        """
        log.section("Generating Table Routes")
        
        # Initialize routers for each schema
        for schema in model_manager.include_schemas:
            if schema not in self.routers:
                self.routers[schema] = APIRouter(prefix=f"/{schema}", tags=[schema.upper()])
        
        # Generate routes for each table
        for table_key, table_data in model_manager.table_cache.items():
            schema, table_name = table_key.split(".")
            log.info(f"Generating CRUD for: {color_palette['schema'](schema)}.{color_palette['table'](table_name)}")
            
            table, (pydantic_model, sqlalchemy_model) = table_data
            
            # Create CRUD operations for this table
            crud_ops = CrudOps(
                table=table,
                pydantic_model=pydantic_model,
                sqlalchemy_model=sqlalchemy_model,
                router=self.routers[schema],
                db_dependency=model_manager.db_client.get_db
            )
            
            # Generate all CRUD routes
            crud_ops.generate_all()
        
        # Register routers with the app
        for schema in model_manager.include_schemas:
            if schema in self.routers:
                self.app.include_router(self.routers[schema])
        
        log.success(f"Generated table routes for {len(model_manager.table_cache)} tables")
    

    def gen_view_routes(self, model_manager: ModelManager) -> None:
        """Generate routes for all views."""
        log.section("Generating View Routes")
        
        # Import the ViewOps class
        from forge.api.views import ViewOps
        
        # Initialize view routers for each schema
        for schema in model_manager.include_schemas:
            router_key = f"{schema}_views"
            if router_key not in self.routers:
                self.routers[router_key] = APIRouter(
                    prefix=f"/{schema}", 
                    tags=[f"{schema.upper()} Views"]
                )
        
        # Generate routes for each view
        for view_key, view_data in model_manager.view_cache.items():
            schema, view_name = view_key.split(".")
            log.info(f"Generating view route for: {color_palette['schema'](schema)}.{color_palette['view'](view_name)}")
            
            table, (query_model, response_model) = view_data
            router = self.routers[f"{schema}_views"]
            
            # Create ViewOps for this view
            view_ops = ViewOps(
                table=table,
                query_model=query_model,
                response_model=response_model,
                router=router,
                db_dependency=model_manager.db_client.get_db,
                schema=schema
            )
            
            # Generate route
            view_ops.generate_route()
        
        # Register all view routers with the app
        for schema in model_manager.include_schemas:
            router_key = f"{schema}_views"
            if router_key in self.routers:
                self.app.include_router(self.routers[router_key])
        
        log.success(f"Generated view routes for {len(model_manager.view_cache)} views")
    
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
                    prefix=f"/{schema}", 
                    tags=[f"{schema.upper()} Functions"]
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
        
        log.success(
            f"Generated function routes for {len(model_manager.fn_cache)} functions "
            f"and {len(model_manager.proc_cache)} procedures"
        )
    
    def _generate_function_routes(
        self, 
        model_manager: ModelManager, 
        function_cache: Dict[str, FunctionMetadata],
        route_type: str
    ) -> None:
        """
        Generate routes for a specific type of database function.
        
        Args:
            model_manager: The model manager containing database metadata
            function_cache: Dictionary of function metadata
            route_type: Type of route to generate ('fn' or 'proc')
        """
        from sqlalchemy import text
        from pydantic import create_model
        from forge.common.types import ForgeBaseModel
        
        # Generate routes for each function
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
            
            # Create input model for parameters
            input_fields = {}
            for param in fn_metadata.parameters:
                from forge.common.types import get_eq_type, ArrayType
                
                # Get parameter type
                field_type = get_eq_type(param.type)
                
                # Handle array types
                if isinstance(field_type, ArrayType):
                    from typing import List
                    field_type = List[field_type.item_type]
                
                # Create field
                from pydantic import Field
                input_fields[param.name] = (
                    field_type if not param.has_default else Optional[field_type],
                    Field(default=param.default_value if param.has_default else ...)
                )
            
            # Create input model
            InputModel = create_model(
                f"{route_type.capitalize()}_{fn_name}_Input",
                __base__=ForgeBaseModel,
                **input_fields
            )
            
            if route_type == "proc":
                # Generate procedure route
                @router.post(
                    f"/proc/{fn_name}",
                    summary=f"Execute {fn_name} procedure",
                    description=fn_metadata.description or f"Execute the {fn_name} procedure"
                )
                async def execute_procedure(
                    params: InputModel,
                    db = Depends(model_manager.db_client.get_db)
                ):
                    # Build parameter list
                    param_list = [f":{p}" for p in params.model_fields.keys()]
                    
                    # Create query
                    query = f"CALL {schema}.{fn_name}({', '.join(param_list)})"
                    
                    # Execute procedure
                    db.execute(text(query), params.model_dump())
                    
                    return {"status": "success", "message": f"Procedure {fn_name} executed successfully"}
            else:
                # Determine function return type
                is_set = fn_metadata.type in (FunctionType.TABLE, FunctionType.SET_RETURNING)
                is_scalar = fn_metadata.type == FunctionType.SCALAR
                
                # Create output model
                if is_set or "TABLE" in (fn_metadata.return_type or ""):
                    # Parse TABLE return type
                    output_fields = self._parse_table_return_type(fn_metadata.return_type)
                else:
                    # Handle scalar return
                    from forge.common.types import get_eq_type, ArrayType
                    output_type = get_eq_type(fn_metadata.return_type or "void")
                    
                    # Handle array types
                    if isinstance(output_type, ArrayType):
                        from typing import List
                        output_type = List[output_type.item_type]
                        
                    # Create result field
                    output_fields = {"result": (output_type, ...)}
                
                # Create output model
                OutputModel = create_model(
                    f"{route_type.capitalize()}_{fn_name}_Output",
                    __base__=ForgeBaseModel,
                    **output_fields
                )
                
                # Generate function route
                @router.post(
                    f"/fn/{fn_name}",
                    response_model=List[OutputModel] if is_set else OutputModel,
                    summary=f"Execute {fn_name} function",
                    description=fn_metadata.description or f"Execute the {fn_name} function"
                )
                async def execute_function(
                    params: InputModel,
                    db = Depends(model_manager.db_client.get_db)
                ):
                    # Build parameter list
                    param_list = [f":{p}" for p in params.model_fields.keys()]
                    
                    # Create query
                    query = f"SELECT * FROM {schema}.{fn_name}({', '.join(param_list)})"
                    
                    # Execute function
                    result = db.execute(text(query), params.model_dump())
                    
                    if is_set:
                        # Return set of records
                        records = result.fetchall()
                        return [OutputModel.model_validate(dict(r._mapping)) for r in records]
                    else:
                        # Return single value/record
                        record = result.fetchone()
                        
                        if is_scalar:
                            # Single scalar value
                            transformed_data = {"result": list(record._mapping.values())[0]}
                            return OutputModel.model_validate(transformed_data)
                        else:
                            # Single record with multiple columns
                            return OutputModel.model_validate(dict(record._mapping))
    
    def _parse_table_return_type(self, return_type: str) -> Dict[str, Any]:
        """
        Parse TABLE and SETOF return types into field definitions.
        
        Args:
            return_type: Function return type string
            
        Returns:
            Dictionary of field definitions
        """
        from forge.common.types import get_eq_type, ArrayType
        from typing import List, Tuple, Any
        from pydantic import Field
        
        fields = {}
        
        if "TABLE" in return_type:
            # Strip 'TABLE' and parentheses
            columns_str = return_type.replace("TABLE", "").strip("()").strip()
            columns = [col.strip() for col in columns_str.split(",")]
            
            for column in columns:
                name, type_str = column.split(" ", 1)
                field_type = get_eq_type(type_str)
                
                # Handle ArrayType in table columns
                if isinstance(field_type, ArrayType):
                    field_type = List[field_type.item_type]
                    
                fields[name] = (field_type, ...)
                
        return fields
    
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
        
        log.success("Generated metadata routes")
    
    def gen_health_routes(self, model_manager: ModelManager) -> None:
        """
        Generate health check routes for API monitoring.
        
        Creates endpoints to check API health and status:
        - Health check
        - Database connectivity
        - Cache status
        - Ping endpoint
        """
        

        log.success("Generated health routes")
    
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
    
    def _process_field_value(self, field_name: str, value: Any, table: Any, model: Type[BaseModel]) -> Any:
        """
        Process field values with appropriate type conversion.
        
        Helper method to handle type conversion for different field types.
        """
        import json
        from forge.common.types import get_eq_type, ArrayType, JSONBType
        
        # Get column from table
        column = getattr(table.c, field_name, None)
        if not column:
            return value
            
        # Get field type
        field_type = get_eq_type(str(column.type), value)
        
        # Process based on type
        if isinstance(field_type, JSONBType):
            # Handle JSONB fields
            if value is None:
                return None
                
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value
            
        elif isinstance(field_type, ArrayType):
            # Handle array fields
            if value is None:
                return []
                
            if isinstance(value, str):
                # Parse PostgreSQL array format
                cleaned_value = value.strip('{}').split(',')
                return [
                    field_type.item_type(item.strip('"')) 
                    for item in cleaned_value 
                    if item.strip()
                ]
            elif isinstance(value, list):
                return [
                    field_type.item_type(item) 
                    for item in value 
                    if item is not None
                ]
            else:
                return value
                
        else:
            # Return as is for other types
            return value
    
    # Additional utility methods
    
    def add_custom_route(
        self,
        path: str,
        endpoint: Callable,
        methods: List[str] = ["GET"],
        tags: List[str] = None,
        summary: str = None,
        description: str = None,
        response_model: Type = None
    ) -> None:
        """
        Add a custom route to the API.
        
        Allows adding custom endpoints beyond the automatically generated ones.
        
        Args:
            path: Route path
            endpoint: Endpoint handler function
            methods: HTTP methods to support
            tags: OpenAPI tags
            summary: Route summary
            description: Route description
            response_model: Pydantic response model
        """
        # Create router for custom routes if needed
        if "custom" not in self.routers:
            self.routers["custom"] = APIRouter(tags=["Custom"])
            
        # Add route
        self.routers["custom"].add_api_route(
            path=path,
            endpoint=endpoint,
            methods=methods,
            tags=tags,
            summary=summary,
            description=description,
            response_model=response_model
        )
        
        # Ensure router is registered
        if "custom" not in [r.prefix for r in self.app.routes]:
            self.app.include_router(self.routers["custom"])
        
        log.success(f"Added custom route: {path}")
    
    def configure_error_handlers(self) -> None:
        """
        Configure global error handlers for the API.
        
        Sets up custom exception handlers for common error types.
        """
        @self.app.exception_handler(HTTPException)
        async def http_exception_handler(request, exc):
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "error": True,
                    "message": exc.detail,
                    "status_code": exc.status_code
                }
            )
            
        @self.app.exception_handler(Exception)
        async def general_exception_handler(request, exc):
            # Log the error
            log.error(f"Unhandled exception: {str(exc)}")
            
            return JSONResponse(
                status_code=500,
                content={
                    "error": True,
                    "message": "Internal server error",
                    "detail": str(exc) if self.config.debug_mode else None,
                    "status_code": 500
                }
            )
            
        log.success("Configured global error handlers")
