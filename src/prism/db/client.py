# """Database client for connecting to and managing database connections."""
# src/prism/db/client.py
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

# You could also move the old DbConfig/PoolConfig dataclasses here if you want.
# For now, we'll keep it simple.


class DbClient:
    """Manages the database connection engine and session creation."""

    def __init__(self, db_url: str):
        # echo=True is essential for debugging our first queries
        self.engine: Engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        print("✅ DbClient initialized and connected to the database.")

    def get_db(self):
        """FastAPI dependency to provide a database session per request."""
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def test_connection(self):
        """Verifies that the connection to the database is alive."""
        try:
            with self.engine.connect() as connection:
                print("✅ Database connection test successful.")
        except Exception as e:
            print(f"❌ Database connection test failed: {e}")
            raise


# import logging
# from contextlib import contextmanager
# from dataclasses import dataclass, field
# from enum import Enum
# from typing import Dict, Generator, List, Optional, Type, Union

# from rich.table import Table
# from sqlalchemy import CursorResult, Inspector, MetaData
# from sqlalchemy import Table as SQLTable
# from sqlalchemy import inspect, text
# from sqlalchemy.engine import Engine, create_engine
# from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

# from prism.ui import console

# # For internal logging, not user-facing output
# log = logging.getLogger(__name__)


# class DbType(str, Enum):
#     """Supported database types."""

#     SQLITE = "sqlite"
#     POSTGRESQL = "postgresql"
#     MYSQL = "mysql"
#     MSSQL = "mssql"


# class DriverType(str, Enum):
#     """Available driver types for database connections."""

#     SYNC = "sync"
#     ASYNC = "async"


# @dataclass
# class PoolConfig:
#     """Database connection pool configuration."""

#     pool_size: int = 5
#     max_overflow: int = 10
#     pool_timeout: int = 30
#     pool_recycle: int = 1800
#     pool_pre_ping: bool = True


# @dataclass
# class DbConfig:
#     """Enhanced database configuration with connection pooling."""

#     db_type: Union[DbType, str]
#     driver_type: Union[DriverType, str]
#     user: Optional[str] = None
#     password: Optional[str] = None
#     host: Optional[str] = None
#     database: str = ""
#     port: Optional[int] = None
#     echo: bool = False
#     pool_config: PoolConfig = field(default_factory=PoolConfig)
#     schema_exclude: List[str] = field(
#         default_factory=lambda: ["information_schema", "pg_catalog"]
#     )
#     ssl_mode: Optional[str] = None

#     def __post_init__(self):
#         """Convert string values to enum types if needed."""
#         if isinstance(self.db_type, str):
#             self.db_type = DbType(self.db_type)
#         if isinstance(self.driver_type, str):
#             self.driver_type = DriverType(self.driver_type)

#     @property
#     def url(self) -> str:
#         """Generate database URL based on configuration."""
#         if self.db_type == DbType.SQLITE:
#             return f"sqlite:///{self.database}"

#         if self.db_type in (DbType.POSTGRESQL, DbType.MYSQL, DbType.MSSQL):
#             if not all([self.user, self.password, self.host, self.database]):
#                 raise ValueError(f"Incomplete configuration for {self.db_type}")

#             dialect = self.db_type.value
#             driver = self._get_driver()
#             port_str = f":{self.port}" if self.port is not None else ""
#             ssl_str = f"?sslmode={self.ssl_mode}" if self.ssl_mode else ""
#             return f"{dialect}{driver}://{self.user}:{self.password}@{self.host}{port_str}/{self.database}{ssl_str}"

#         raise ValueError(f"Unsupported database type: {self.db_type}")

#     def _get_driver(self) -> str:
#         """Get appropriate database driver based on configuration."""
#         match self.db_type:
#             case DbType.POSTGRESQL:
#                 return (
#                     "+asyncpg" if self.driver_type == DriverType.ASYNC else "+psycopg2"
#                 )
#             case DbType.MYSQL:
#                 return (
#                     "+aiomysql" if self.driver_type == DriverType.ASYNC else "+pymysql"
#                 )
#             case DbType.MSSQL:
#                 return "+pytds" if self.driver_type == DriverType.ASYNC else "+pyodbc"
#             case _:
#                 return ""


# class DbClient:
#     """Database client for handling connections and queries."""

#     def __init__(self, config: DbConfig):
#         self.config = config
#         self.engine = self._create_engine()
#         self.SessionLocal = sessionmaker(
#             autocommit=False, autoflush=False, bind=self.engine
#         )
#         self.metadata = MetaData()
#         self.Base = self._create_base()
#         self._load_metadata()

#     def _create_engine(self) -> Engine:
#         """Create SQLAlchemy engine with connection pooling."""
#         pool_kwargs = self.config.pool_config.__dict__
#         return create_engine(self.config.url, echo=self.config.echo, **pool_kwargs)

#     def _create_base(self) -> Type[DeclarativeBase]:
#         """Create a declarative base class for models."""

#         class Base(DeclarativeBase):
#             pass

#         return Base

#     def _load_metadata(self) -> None:
#         """Load database metadata with schema filtering."""
#         inspector: Inspector = inspect(self.engine)
#         for schema in sorted(
#             set(inspector.get_schema_names()) - set(self.config.schema_exclude)
#         ):
#             for t in inspector.get_table_names(schema=schema):
#                 SQLTable(t, self.metadata, autoload_with=self.engine, schema=schema)
#             for v in inspector.get_view_names(schema=schema):
#                 SQLTable(v, self.metadata, autoload_with=self.engine, schema=schema)

#     def test_connection(self) -> None:
#         """Test database connection and log connection info."""
#         try:
#             with self.engine.connect() as connection:
#                 result = connection.execute(
#                     text("SELECT current_user, current_database()")
#                 ).fetchone()
#                 if result:
#                     user, database = result
#                     console.print(
#                         f"Connection successful to [bold blue]{database}[/] as [bold green]{user}[/]"
#                     )
#         except Exception as e:
#             console.print(f"[bold red]Database connection test failed:[/] {e}")
#             raise

#     @contextmanager
#     def get_session(self) -> Generator[Session, None, None]:
#         """Context manager for database sessions."""
#         session = self.SessionLocal()
#         try:
#             yield session
#             session.commit()
#         except Exception:
#             session.rollback()
#             raise
#         finally:
#             session.close()

#     def get_db(self) -> Generator[Session, None, None]:
#         """Generator for database sessions (FastAPI dependency)."""
#         db = self.SessionLocal()
#         try:
#             yield db
#         finally:
#             db.close()

#     def exec_raw_sql(self, query: str) -> CursorResult:
#         """Execute raw SQL query."""
#         with self.engine.connect() as connection:
#             return connection.execute(text(query))

#     def get_db_version(self) -> str:
#         """Get database version information."""
#         try:
#             match self.config.db_type:
#                 case DbType.POSTGRESQL | DbType.MYSQL:
#                     query = "SELECT version()"
#                 case DbType.SQLITE:
#                     query = "SELECT sqlite_version()"
#                 case DbType.MSSQL:
#                     query = "SELECT @@VERSION"
#                 case _:
#                     return "Unknown database type"
#             version_info = str(self.exec_raw_sql(query).scalar())
#             return version_info.split("\n")[0]
#         except Exception as e:
#             console.print(f"[red]Failed to get database version:[/] {e}")
#             return "Unknown"

#     def analyze_table_relationships(self) -> Dict[str, List[Dict[str, str]]]:
#         """Analyze and return table relationships."""
#         relationships = {}
#         for table_name, table in self.metadata.tables.items():
#             relationships[table_name] = []
#             for fk in table.foreign_keys:
#                 relationships[table_name].append(
#                     {
#                         "from_col": fk.parent.name,
#                         "to_table": fk.column.table.name,
#                         "to_col": fk.column.name,
#                     }
#                 )
#         return relationships

#     def log_metadata_stats(self):
#         """Log database connection metadata and statistics with a clean, indented layout."""
#         result = self.exec_raw_sql("SELECT current_user, current_database()").fetchone()
#         if not result:
#             console.print("[red]Could not retrieve user and database for stats.[/]")
#             return

#         user, database = result
#         db_version = self.get_db_version()

#         console.print("[bold italic]Database Connection Info[/bold italic]")

#         info_data = [
#             ("Version", f"[white]{db_version}[/]"),
#             ("Type", f"[green]{self.config.db_type.name}[/]"),
#             ("Driver", f"[green]{self.config.driver_type.name}[/]"),
#             ("Database", f"[blue]{database}[/]"),
#             ("User", f"[green]{user}[/]"),
#             ("Host", f"[blue]{self.config.host}:{self.config.port}[/]"),
#         ]

#         for label, value in info_data:
#             # Manually construct each line for precise control
#             # 4 spaces for indentation, -12 for left-aligned label column
#             console.print(f"    {label:<12}{value}")

#         console.print()  # Add a blank line for spacing
