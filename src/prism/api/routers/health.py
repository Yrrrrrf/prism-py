# src/prism/api/routers/health.py
from datetime import datetime, timezone

from fastapi import APIRouter, FastAPI, HTTPException
from pydantic import BaseModel, Field

from prism.ui import console


class HealthStatus(BaseModel):
    """Response model for the main health check."""

    status: str = "ok"
    uptime: str
    db_status: str
    db_error: str | None = Field(
        None, description="Details of DB connection error, if any."
    )


class HealthGenerator:
    """Generates health check routes for API monitoring."""

    def __init__(self, app: FastAPI, prism_instance: "ApiPrism"):
        self.app = app
        self.prism = prism_instance
        self.router = APIRouter(prefix="/health", tags=["Health"])

    def generate_routes(self):
        """Creates and registers all health-related endpoints."""

        @self.router.get("/ping", summary="Simple connectivity check")
        async def ping() -> dict[str, str]:
            """A basic endpoint that always returns 'ok' to confirm the server is running."""
            return {"result": "pong"}

        @self.router.get(
            "/", response_model=HealthStatus, summary="Full service health check"
        )
        async def get_health() -> HealthStatus:
            """
            Provides a detailed health status, including API uptime and database connectivity.
            """
            uptime = str(datetime.now(timezone.utc) - self.prism.start_time)
            db_ok = False
            db_err = None
            try:
                # Use the test_connection method which is a synchronous call
                with self.prism.db_client.engine.connect() as connection:
                    db_ok = True
            except Exception as e:
                db_err = str(e)

            return HealthStatus(
                uptime=uptime, db_status="ok" if db_ok else "error", db_error=db_err
            )

        @self.router.post(
            "/clear-cache", summary="Clear and reload introspection cache"
        )
        async def clear_cache() -> dict[str, str]:
            """
            Clears the internal database metadata cache and re-runs introspection.
            Useful after making schema changes to the database without restarting the API.
            """
            console.print(
                "[bold yellow]⚠️  Clearing introspection cache and re-running...[/]"
            )
            try:
                # This calls the method back on the main ApiPrism instance
                self.prism._introspect_all(self.prism.schemas)
                return {
                    "status": "success",
                    "message": "Introspection cache cleared and reloaded.",
                }
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Failed to clear cache: {e}"
                )

        # Register the completed router with the main FastAPI application
        self.app.include_router(self.router)
