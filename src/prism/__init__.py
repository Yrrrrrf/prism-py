"""
prism-py: Generate FastAPI routes automatically from database schemas.
"""

from fastapi import FastAPI

# from prism.api import MetadataRouter
# from prism.db import DbClient, DbConfig, PoolConfig
from prism.prism import ApiPrism

__version__ = "0.0.5"


# Create the function that was being imported
def prism_init() -> str:
    """Initialize prism and return version information."""
    # print("ALL LIBRARIES SETUP AS EXPECTED!!!...")
    print(f"prism-py initialized (version {__version__})")
    return __version__


def main() -> None:
    """Main entry point for the GWA CLI."""
    # todo: Create some CLI app that allow the user to interact with prism
    # init_cli()
    print("Welcome to prism-py!")


if __name__ == "__main__":
    main()
