# examples/dev.py
"""
Simple script that validates if all the dependencies are installed and imports the main module.
"""


def handle_deps():
    from fastapi import FastAPI
    from pydantic import BaseModel


def handle_forge():
    from forge import forge_init

    forge_init()


if __name__ == "__main__":
    handle_deps()
    handle_forge()
