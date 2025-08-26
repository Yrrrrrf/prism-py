# src/prism/db/client.py
from typing import Any

from sqlalchemy import CursorResult, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from prism.ui import console


class DbClient:
    """Manages the database connection engine and session creation."""

    def __init__(self, db_url: str):
        """
        Initializes the DbClient.

        Args:
            db_url: The full database connection string (e.g., "postgresql://user:pass@host/db").
        """
        self.engine: Engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.test_connection()  # Test connection on initialization

    def get_db(self):
        """FastAPI dependency to provide a database session per request."""
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def test_connection(self) -> None:
        """
        Tests the database connection and logs a simple success or failure message.
        For more detailed stats, use `log_connection_stats`.
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text("SELECT current_user, current_database()")
                ).fetchone()
                if result:
                    user, database = result
                    console.print(
                        f"✅ Connection successful to [bold blue]{database}[/] as [bold green]{user}[/]"
                    )
        except Exception as e:
            console.print(f"❌ [bold red]Database connection test failed:[/] {e}")
            raise

    def exec_raw_sql(
        self, query: str, params: dict[str, Any] | None = None
    ) -> CursorResult:
        """
        Execute a raw, parameterized SQL query.

        Args:
            query: The SQL query string to execute.
            params: A dictionary of parameters to bind to the query.

        Returns:
            A SQLAlchemy CursorResult object.
        """
        with self.engine.connect() as connection:
            return connection.execute(text(query), params or {})

    def get_db_version(self) -> str:
        """
        Get database version information by inspecting the engine's dialect.
        """
        # The new approach: get the dialect name directly from the engine.
        dialect = self.engine.dialect.name

        try:
            # Use a match statement on the dialect name string.
            match dialect:
                case "postgresql" | "mysql":
                    query = "SELECT version()"
                case "sqlite":
                    query = "SELECT sqlite_version()"
                case "mssql":
                    query = "SELECT @@VERSION"
                case _:
                    return f"Unknown database type ('{dialect}')"

            version_info = str(self.exec_raw_sql(query).scalar())
            return version_info.split("\n")[0]  # Return the first line for cleanliness
        except Exception as e:
            console.print(f"[red]Failed to get database version:[/] {e}")
            return "Unknown"

    def log_connection_stats(self):
        """
        Logs detailed database connection metadata and statistics in a rich table format.
        """
        try:
            # Get User and Database Name
            result = self.exec_raw_sql(
                "SELECT current_user, current_database()"
            ).fetchone()
            if not result:
                console.print("[red]Could not retrieve user and database for stats.[/]")
                return
            user, database = result

            # Get all other info directly from the engine object
            db_version = self.get_db_version()
            dialect_name = self.engine.dialect.name
            driver_name = self.engine.dialect.driver
            host = self.engine.url.host
            port = self.engine.url.port

            console.rule("[bold blue]Database Connection Info", style="blue")

            info_data = [
                ("Version", f"[white]{db_version}[/]"),
                ("Type", f"[green]{dialect_name}[/]"),
                ("Driver", f"[green]{driver_name}[/]"),
                ("Database", f"[blue]{database}[/]"),
                ("User", f"[green]{user}[/]"),
                (
                    "Host",
                    f"[blue]{host}:{port}[/]" if host else "[dim]N/A (e.g., SQLite)[/]",
                ),
            ]

            for label, value in info_data:
                # Manually construct each line for precise control
                console.print(f"    {label:<12}{value}")

            console.print()  # Add a blank line for spacing
        except Exception as e:
            console.print(f"❌ [bold red]Failed to log connection stats:[/] {e}")
