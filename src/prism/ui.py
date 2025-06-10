# src/prism/ui.py

from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from sqlalchemy import Column, Table as SQLTable
from sqlalchemy import Enum as SQLAlchemyEnum

from .common.types import FunctionMetadata, JSONBType, get_eq_type

# --- Global Console ---
# All modules will import this single console instance.
console = Console()

# --- Re-implementation of print helpers using Rich ---


def display_table_structure(table: SQLTable) -> None:
    """Prints detailed table structure using a rich Table."""

    structure_table = Table(
        box=None, padding=(0, 1), show_header=False, show_edge=False
    )
    structure_table.add_column("Name", style="cyan", no_wrap=True, width=24)
    structure_table.add_column("Type", style="green", width=32)
    structure_table.add_column("Details", style="white")

    for column in table.columns:
        col_name = f"{column.name}{'*' if not column.nullable else ''}"
        col_type = str(column.type)

        details = []
        if column.primary_key:
            details.append("[yellow]PK[/yellow]")
        if column.foreign_keys:
            fk = next(iter(column.foreign_keys))
            details.append(
                f"[blue]FK -> {fk.column.table.schema}.{fk.column.table.name}[/blue]"
            )

        if isinstance(column.type, SQLAlchemyEnum):
            details.append(
                f"[magenta]Enum[/magenta]: {', '.join(map(str, column.type.enums))}"
            )

        py_type = get_eq_type(str(column.type))
        if isinstance(py_type, JSONBType):
            details.append("[bold magenta]JSONB[/bold magenta]")

        structure_table.add_row(col_name, col_type, " ".join(details))

    console.print(structure_table)
    console.print()


def display_function_structure(fn_metadata: FunctionMetadata) -> None:
    """Prints detailed function/procedure structure using rich."""

    return_type = fn_metadata.return_type or "void"
    fn_type = str(fn_metadata.type).split(".")[-1].upper()
    console.print(
        f"  [bold]Returns[/bold]: [magenta]{return_type}[/] [dim]({fn_type})[/dim]"
    )

    if fn_metadata.description:
        console.print(f"  [dim]{fn_metadata.description}[/dim]")

    if fn_metadata.parameters:
        params_table = Table(box=None, show_header=False, padding=(0, 1, 0, 4))
        params_table.add_column("Name", style="cyan", width=22)
        params_table.add_column("Type", style="green", width=28)
        params_table.add_column("Details", style="white")

        for param in fn_metadata.parameters:
            mode_str = f"[bold yellow]{param.mode}[/bold yellow]"
            default_str = (
                f" [dim]DEFAULT {param.default_value}[/dim]"
                if param.has_default
                else ""
            )
            params_table.add_row(param.name, param.type, f"{mode_str}{default_str}")
        console.print(params_table)
    console.print()


def print_welcome(project_name: str, version: str, host: str, port: int) -> None:
    """Prints a welcome message using a rich Panel."""
    docs_url = f"http://{host}:{port}/docs"
    message = Text.from_markup(
        f"API Documentation available at [link={docs_url}]{docs_url}[/link]"
    )
    panel = Panel(
        Align.center(message, vertical="middle"),
        title=f"[bold green]{project_name} v{version}[/bold green]",
        border_style="blue",
        padding=(1, 2),
    )
    console.print(panel)
