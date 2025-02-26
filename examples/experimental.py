# examples/main.py
"""
forge-py: Generate FastAPI routes automatically from database schemas.
"""

# Re-export main components for cleaner imports
from fastapi import FastAPI
from forge.core.logging import log, color_palette
from forge.core.config import ForgeConfig
from forge import forge_init


# ? Main API Forge -----------------------------------------------------------------------------------

# print("Importing main.py...")

# app: FastAPI = FastAPI()  # * Create a FastAPI app (needed when calling the script directly)
# Configuration
config = ForgeConfig(project_name="My API", version=forge_init, author="Your Name")

# Logging
log.section("Starting Application")

with log.timed("Database initialization"):
    # Database initialization code here
    log.info(f"Connected to database as {color_palette['schema']('public')}")

    with log.indented():
        log.success(f"Found {color_palette['table']('users')} table")
        log.success(f"Found {color_palette['view']('active_users')} view")

# Print a table
log.table(
    headers=["Schema", "Tables", "Views"], rows=[["public", 12, 5], ["auth", 4, 2]]
)


def run():
    forge_init()
    # * ... Some other code here...


if __name__ == "__main__":
    run()
