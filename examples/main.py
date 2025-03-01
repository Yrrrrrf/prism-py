# examples/main.py
"""
prism-py: Generate FastAPI routes automatically from database schemas.
"""

# Re-export main components for cleaner imports
from fastapi import FastAPI

from prism import prism_init

# ? Main API Prism -----------------------------------------------------------------------------------

print("Importing main.py...")

app: FastAPI = (
    FastAPI()
)  # * Create a FastAPI app (needed when calling the script directly)


def run():
    prism_init()
    # * ... Some other code here...


if __name__ == "__main__":
    run()
