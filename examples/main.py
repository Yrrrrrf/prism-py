# examples/main.py
"""
forge-py: Generate FastAPI routes automatically from database schemas.
"""

# Re-export main components for cleaner imports
from fastapi import FastAPI
from forge import forge_init


# ? Main API Forge -----------------------------------------------------------------------------------

print("Importing main.py...")

app: FastAPI = (
    FastAPI()
)  # * Create a FastAPI app (needed when calling the script directly)


def run():
    forge_init()
    # * ... Some other code here...


if __name__ == "__main__":
    run()
