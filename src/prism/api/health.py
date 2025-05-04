# src/prism/api/health.py
from datetime import datetime

from fastapi import APIRouter, Response
from pydantic import BaseModel

from prism.api.router import RouteGenerator
from prism.db.models import ModelManager


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


class HealthGenerator(RouteGenerator):
    """Generator for health check routes."""

    def __init__(
        self,
        router: APIRouter,
        model_manager: ModelManager,
        version: str,
        start_time: datetime,
    ):
        super().__init__(
            resource_name="health",
            router=router,
            db_dependency=model_manager.db_client.get_db,
            schema=None,
            response_model=None,  # Not needed for health routes
            query_model=None,  # Not needed for health routes
            table=None,  # Not needed for health routes
        )
        self.model_manager = model_manager
        self.version = version
        self.start_time = start_time

    def generate_routes(self):
        """Generate all health routes."""
        self.health_check()
        self.ping()
        self.cache_status()
        self.clear_cache()

    def health_check(self):
        """Generate health check route."""

        @self.router.get(
            "",
            response_model=HealthResponse,
            summary="Health check",
            description="Get the current health status of the API",
        )
        async def health_check():
            """Basic health check endpoint."""
            # Check database connection
            is_connected = False
            try:
                # todo: Remove the print statement in production
                # todo: Check the statement on the db_client.test_connection method!
                self.model_manager.db_client.test_connection()
                is_connected = True
            except Exception:
                pass

            # Calculate uptime
            uptime = (datetime.now() - self.start_time).total_seconds()

            return HealthResponse(
                status="healthy" if is_connected else "degraded",
                timestamp=datetime.now(),
                version=self.version,
                uptime=uptime,
                database_connected=is_connected,
            )

    def ping(self):
        """Generate ping route."""

        @self.router.get(
            "/ping",
            summary="Ping",
            description="Simple ping endpoint for load balancers",
        )
        async def ping():
            """Simple ping endpoint for load balancers."""
            return Response(content="pong", media_type="text/plain")

    def cache_status(self):
        """Generate cache status route."""

        @self.router.get(
            "/cache",
            response_model=CacheStatus,
            summary="Cache status",
            description="Get metadata cache status",
        )
        async def cache_status():
            """Get metadata cache status."""
            counter = [
                len(self.model_manager.table_cache),
                len(self.model_manager.view_cache),
                len(self.model_manager.enum_cache),
                len(self.model_manager.fn_cache),
                len(self.model_manager.proc_cache),
                len(self.model_manager.trig_cache),
            ]

            return CacheStatus(
                last_updated=self.start_time,
                total_items=sum(counter),
                tables_cached=counter[0],
                views_cached=counter[1],
                enums_cached=counter[2],
                functions_cached=counter[3],
                procedures_cached=counter[4],
                triggers_cached=counter[5],
            )

    def clear_cache(self):
        """Generate clear cache route."""

        @self.router.post(
            "/clear-cache",
            summary="Clear cache",
            description="Clear and reload metadata cache",
        )
        async def clear_cache():
            """Clear and reload metadata cache."""
            try:
                # Reload each cache
                self.model_manager._load_models()
                self.model_manager._load_enums()
                self.model_manager._load_views()
                self.model_manager._load_functions()

                return {
                    "status": "success",
                    "message": "Cache cleared and reloaded successfully",
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to reload cache: {str(e)}",
                }
