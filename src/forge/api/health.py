
    # def gen_health_routes(self, model_manager: ModelManager) -> None:
    #     """
    #     Generate health check routes for API monitoring.
        
    #     Creates endpoints to check API health and status:
    #     - Health check
    #     - Database connectivity
    #     - Cache status
    #     - Ping endpoint
    #     """
    #     log.section("Generating Health Routes")
        
    #     # Create health router
    #     router = APIRouter(prefix="/health", tags=["Health"])
        
    #     @router.get(
    #         "",
    #         response_model=HealthResponse,
    #         summary="Health check",
    #         description="Get the current health status of the API"
    #     )
    #     async def health_check():
    #         """Basic health check endpoint."""
    #         # Check database connection
    #         is_connected = False
    #         try:
    #             model_manager.db_client.test_connection()
    #             is_connected = True
    #         except Exception:
    #             pass
                
    #         # Calculate uptime
    #         uptime = (datetime.now() - self.start_time).total_seconds()
                
    #         return HealthResponse(
    #             status="healthy" if is_connected else "degraded",
    #             timestamp=datetime.now(),
    #             version=self.config.version,
    #             uptime=uptime,
    #             database_connected=is_connected
    #         )
        
    #     @router.get(
    #         "/ping",
    #         response_class=PlainTextResponse,
    #         summary="Ping",
    #         description="Simple ping endpoint for load balancers"
    #     )
    #     async def ping():
    #         """Simple ping endpoint for load balancers."""
    #         return "pong"
        
    #     @router.get(
    #         "/cache",
    #         response_model=CacheStatus,
    #         summary="Cache status",
    #         description="Get metadata cache status"
    #     )
    #     async def cache_status():
    #         """Get metadata cache status."""
    #         counter = [
    #             len(model_manager.table_cache),
    #             len(model_manager.view_cache),
    #             len(model_manager.enum_cache),
    #             len(model_manager.fn_cache),
    #             len(model_manager.proc_cache),
    #             len(model_manager.trig_cache),
    #         ]
            
    #         return CacheStatus(
    #             last_updated=self.start_time,
    #             total_items=sum(counter),
    #             tables_cached=counter[0],
    #             views_cached=counter[1],
    #             enums_cached=counter[2],
    #             functions_cached=counter[3],
    #             procedures_cached=counter[4],
    #             triggers_cached=counter[5],
    #         )
        
    #     @router.post(
    #         "/clear-cache",
    #         summary="Clear cache",
    #         description="Clear and reload metadata cache"
    #     )
    #     async def clear_cache():
    #         """Clear and reload metadata cache."""
    #         try:
    #             # Reload each cache
    #             model_manager._load_models()
    #             model_manager._load_enums()
    #             model_manager._load_views()
    #             model_manager._load_functions()
                
    #             return {
    #                 "status": "success",
    #                 "message": "Cache cleared and reloaded successfully"
    #             }
    #         except Exception as e:
    #             return {
    #                 "status": "error",
    #                 "message": f"Failed to reload cache: {str(e)}"
    #             }
        
    #     # Register the router with the app
    #     self.app.include_router(router)
        
    #     log.success("Generated health routes")
    