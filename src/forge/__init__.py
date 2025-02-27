"""
forge-py: Generate FastAPI routes automatically from database schemas.
"""
from fastapi import FastAPI


from forge.db import DbClient, DbConfig, PoolConfig
from forge.forge import ApiForge, ForgeConfig, ModelManager
from forge.api import MetadataRouter


__version__ = "0.0.1"


# Create the function that was being imported
def forge_init() -> str:
    """Initialize forge and return version information."""
    # print("ALL LIBRARIES SETUP AS EXPECTED!!!...")
    print(f"Forge-py initialized (version {__version__})")
    return __version__
