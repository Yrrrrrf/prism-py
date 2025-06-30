<h1 align="center">
  <img src="https://raw.githubusercontent.com/Yrrrrrf/prism-py/main/resources/img/prism.png" alt="Prism Icon" width="128" height="128" description="A prism that can take one light source and split it into multiple colors!">
  <div align="center">prism-py</div>
</h1>

<div align="center">

[![PyPI version](https://img.shields.io/pypi/v/prism-py)](https://pypi.org/project/prism-py/)
[![GitHub: Prism-py](https://img.shields.io/badge/GitHub-prism--py-181717?logo=github)](https://github.com/Yrrrrrf/prism-py)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://choosealicense.com/licenses/mit/)
[![Downloads](https://pepy.tech/badge/prism-py)](https://pepy.tech/project/prism-py)

</div>

## Overview

**prism-py** is a Python library for automatic API generation from database schemas. It creates a complete REST API that mirrors your database structure, handling tables, views, functions, and procedures with proper type safety and validation.

Built on top of [FastAPI](https://fastapi.tiangolo.com/), prism-py eliminates boilerplate code and provides a comprehensive type system for API development, allowing you to focus on business logic instead of API structure.

> **Note:** This library is part of the Prism ecosystem, which includes [**prism-ts**](https://github.com/Yrrrrrf/prism-ts), a TypeScript client library that consumes prism-py APIs with full type safety.

## Key Features

- **Automatic Route Generation**: Create CRUD endpoints for tables, views, functions, and procedures
- **Composite Primary Keys**: Full support for tables with multi-column primary keys.
- **String Length Validation**: Automatic server-side validation based on database schema (e.g., `VARCHAR(50)`).
- **Type Safety**: Full type handling with proper conversions between SQL and Python types
- **Database Independence**: Support for PostgreSQL, MySQL, and SQLite
- **Schema-Based Organization**: Routes organized by database schemas for clean API structure
- **Enhanced Filtering**: Sorting, pagination, and complex query support
- **Metadata API**: Explore your database structure programmatically
- **Health Monitoring**: Built-in health check endpoints
- **Zero Boilerplate**: Generate complete APIs with minimal code

## Installation

```bash
pip install prism-py
```

## Quick Start

Here's a minimal example to get you started:

```python
from fastapi import FastAPI
from prism import ApiPrism, PrismConfig, DbClient, DbConfig, PoolConfig, ModelManager

# Initialize FastAPI app
app = FastAPI()

# Configure database connection
db_client = DbClient(
    config=DbConfig(
        db_type="postgresql",
        driver_type="sync",
        database="yourdb",
        user="username",
        password="password",
        host="localhost",
        port=5432,
        pool_config=PoolConfig(
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True
        )
    )
)

# Create model manager with selected schemas
model_manager = ModelManager(
    db_client=db_client,
    include_schemas=["public", "app"]
)

# Initialize API generator
api_prism = ApiPrism(
    config=PrismConfig(
        project_name="My API",
        version="1.0.0",
        description="Auto-generated API for my database"
    ),
    app=app
)

# Generate all routes
api_prism.generate_all_routes(model_manager)

# Print welcome message with API documentation link
api_prism.print_welcome(db_client)
```

## Generated Routes

prism-py automatically creates the following types of routes:

### Table Routes
- `POST /{schema}/{table}` - Create a record
- `GET /{schema}/{table}` - Read records with filtering
- `PUT /{schema}/{table}` - Update records
- `DELETE /{schema}/{table}` - Delete records

### View Routes
- `GET /{schema}/{view}` - Read from view with optional filtering

### Function/Procedure Routes
- `POST /{schema}/fn/{function}` - Execute database function
- `POST /{schema}/proc/{procedure}` - Execute stored procedure

### Metadata Routes
- `GET /dt/schemas` - List all database schemas and structure
- `GET /dt/{schema}/tables` - List all tables in a schema
- `GET /dt/{schema}/views` - List all views in a schema
- `GET /dt/{schema}/functions` - List all functions in a schema
- `GET /dt/{schema}/procedures` - List all procedures in a schema

### Health Routes
- `GET /health` - Get API health status
- `GET /health/ping` - Basic connectivity check
- `GET /health/cache` - Check metadata cache status
- `POST /health/clear-cache` - Clear and reload metadata cache

## Usage Examples

See the [examples](./examples) directory for complete sample applications:

- **[Hub Example](./examples/hub.py)**: Shows complex database integration with multiple schemas
- **[Basic Example](./examples/main.py)**: Demonstrates essential setup and configuration

## The Prism Ecosystem

prism-py is part of the Prism ecosystem, designed to create a seamless bridge between your database and type-safe client applications:

- **prism-py** (Python): Server-side library for automatic API generation
- **prism-ts** (TypeScript, formerly ts-prism): Client-side library for consuming prism-py APIs with full type safety

Together, these libraries enable end-to-end type safety and eliminate boilerplate code across your full stack.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

<!-- todo: Add some simple example but using some SQLite database, so that users can try it out without needing to set up a database server. -->
<!-- todo: This will also allow for a simpler setup for the examples. -->
