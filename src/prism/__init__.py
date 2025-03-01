"""
prism-py: Generate FastAPI routes automatically from database schemas.
"""

from fastapi import FastAPI

from prism.api import MetadataRouter
from prism.db import DbClient, DbConfig, PoolConfig
from prism.prism import ApiPrism, ModelManager, PrismConfig

__version__ = "0.0.1"


# Create the function that was being imported
def prism_init() -> str:
    """Initialize prism and return version information."""
    # print("ALL LIBRARIES SETUP AS EXPECTED!!!...")
    print(f"prism-py initialized (version {__version__})")
    return __version__
