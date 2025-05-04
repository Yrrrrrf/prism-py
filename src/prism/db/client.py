"""Database client for connecting to and managing database connections."""

from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Generator, List, Optional, Type, Union

from sqlalchemy import CursorResult, Inspector, MetaData, Table, inspect, text
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from prism.core.logging import *


class DbType(str, Enum):
    """Supported database types."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"


class DriverType(str, Enum):
    """Available driver types for database connections."""

    SYNC = "sync"
    ASYNC = "async"


@dataclass
class PoolConfig:
    """Database connection pool configuration."""

    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 1800
    pool_pre_ping: bool = True


@dataclass
class DbConfig:
    """Enhanced database configuration with connection pooling."""

    db_type: Union[DbType, str]
    driver_type: Union[DriverType, str]
    user: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    database: str = None
    port: Optional[int] = None
    echo: bool = False
    pool_config: PoolConfig = field(default_factory=PoolConfig)
    schema_exclude: List[str] = field(
        default_factory=lambda: ["information_schema", "pg_catalog"]
    )
    ssl_mode: Optional[str] = None

    def __post_init__(self):
        """Convert string values to enum types if needed."""
        if isinstance(self.db_type, str):
            self.db_type = DbType(self.db_type)
        if isinstance(self.driver_type, str):
            self.driver_type = DriverType(self.driver_type)

    @property
    def url(self) -> str:
        """Generate database URL based on configuration."""
        if self.db_type == DbType.SQLITE:
            return f"sqlite:///{self.database}"

        if self.db_type in (DbType.POSTGRESQL, DbType.MYSQL, DbType.MSSQL):
            if not all([self.user, self.password, self.host]):
                raise ValueError(f"Incomplete configuration for {self.db_type}")

            dialect = self.db_type.value
            driver = self._get_driver()

            port_str = f":{self.port}" if self.port is not None else ""
            ssl_str = f"?sslmode={self.ssl_mode}" if self.ssl_mode else ""

            return f"{dialect}{driver}://{self.user}:{self.password}@{self.host}{port_str}/{self.database}{ssl_str}"

        raise ValueError(f"Unsupported database type: {self.db_type}")

    def _get_driver(self) -> str:
        """Get appropriate database driver based on configuration."""
        match self.db_type:
            case DbType.POSTGRESQL:
                return (
                    "+asyncpg" if self.driver_type == DriverType.ASYNC else "+psycopg2"
                )
            case DbType.MYSQL:
                return (
                    "+aiomysql" if self.driver_type == DriverType.ASYNC else "+pymysql"
                )
            case DbType.MSSQL:
                return "+pytds" if self.driver_type == DriverType.ASYNC else "+pyodbc"
            case _:
                return ""


class DbClient:
    """Database client for handling connections and queries."""

    def __init__(self, config: DbConfig):
        self.config = config
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.metadata = MetaData()
        self.Base = self._create_base()
        self._load_metadata()

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling."""
        pool_kwargs = (
            self.config.pool_config.__dict__ if self.config.pool_config else {}
        )
        return create_engine(self.config.url, echo=self.config.echo, **pool_kwargs)

    def _create_base(self) -> Type[DeclarativeBase]:
        """Create a declarative base class for models."""

        class Base(DeclarativeBase):
            pass

        return Base

    def _load_metadata(self) -> None:
        """Load database metadata with schema filtering."""
        inspector: Inspector = inspect(self.engine)

        for schema in sorted(
            set(inspector.get_schema_names()) - set(self.config.schema_exclude)
        ):
            [
                Table(t, self.metadata, autoload_with=self.engine, schema=schema)
                for t in inspector.get_table_names(schema=schema)
            ]
            [
                Table(v, self.metadata, autoload_with=self.engine, schema=schema)
                for v in inspector.get_view_names(schema=schema)
            ]

    def test_connection(self) -> None:
        """Test database connection and log connection info."""
        try:
            user, database = self.exec_raw_sql(
                "SELECT current_user, current_database()"
            ).fetchone()
            # todo: Check on a best way to log the connection info!
            # log.info(
            #     f"Connected to {info_style(database)} database as {success_style(user)}"
            # )
            # log.info(f"Connected to database as {color_palette['schema'](user)}")
            return (user, database)
        except Exception as e:
            log.error(f"Database connection test failed: {str(e)}")
            raise

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager for database sessions."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_db(self) -> Generator[Session, None, None]:
        """Generator for database sessions (FastAPI dependency)."""
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def exec_raw_sql(self, query: str) -> CursorResult:
        """Execute raw SQL query."""
        with self.engine.connect() as connection:
            return connection.execute(text(query))

    def get_db_version(self) -> str:
        """Get database version information."""
        try:
            match self.config.db_type:
                case DbType.POSTGRESQL | DbType.MYSQL:
                    query = "SELECT version()"
                case DbType.SQLITE:
                    query = "SELECT sqlite_version()"
                case DbType.MSSQL:
                    query = "SELECT @@VERSION"
                case _:
                    return "Unknown database type"

            version_info = str(self.exec_raw_sql(query).scalar())
            return version_info.split("\n")[0]  # First line only
        except Exception as e:
            log.error(f"Failed to get database version: {str(e)}")
            return "Unknown"

    def analyze_table_relationships(self) -> Dict[str, List[Dict[str, str]]]:
        """Analyze and return table relationships."""
        relationships = {}
        for table_name, table in self.metadata.tables.items():
            relationships[table_name] = []
            for fk in table.foreign_keys:
                relationships[table_name].append(
                    {
                        "from_col": fk.parent.name,
                        "to_table": fk.column.table.name,
                        "to_col": fk.column.name,
                    }
                )
        return relationships

    def log_metadata_stats(self):
        """Log database connection metadata and statistics."""
        user, database = self.exec_raw_sql(
            "SELECT current_user, current_database()"
        ).fetchone()
        db_version = self.get_db_version()

        connection_info = [
            ("Version:", db_version, bold),
            ("Type:", self.config.db_type.name, green),
            ("Driver:", self.config.driver_type.name, green),
            ("DB:", database, info_style),
            ("Host:", f"{self.config.host}:{self.config.port}", info_style),
        ]

        [
            print(f"\t{dim(italic(f'{label:<12}'))} {formatter(value)}")
            for label, value, formatter in connection_info
        ]
        print()
