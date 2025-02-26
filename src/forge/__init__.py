"""
forge-py: Generate FastAPI routes automatically from database schemas.
"""
# # Re-export main components for cleaner imports
# from forge.forge import ApiForge, ForgeConfig
# from forge.tools.db import DbClient, DbConfig, PoolConfig
# from forge.tools.model import ModelManager

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


# # Define what's available at the top level
# __all__ = [
#     "ApiForge",
#     "ForgeConfig",
#     "DbClient",
#     "DbConfig",
#     "PoolConfig",
#     "ModelManager",
# ]
