# src/prism/cache.py
from dataclasses import dataclass, field
from typing import Dict, List

from prism.core.models.enums import EnumInfo
from prism.core.models.functions import FunctionMetadata
from prism.core.models.tables import TableMetadata
from prism.ui import console, Table


@dataclass
class SchemaCache:
    """A type-safe container for all introspected objects within a single database schema."""

    tables: List[TableMetadata] = field(default_factory=list)
    views: List[TableMetadata] = field(default_factory=list)
    enums: Dict[str, EnumInfo] = field(default_factory=dict)
    functions: List[FunctionMetadata] = field(default_factory=list)
    procedures: List[FunctionMetadata] = field(default_factory=list)
    triggers: List[FunctionMetadata] = field(default_factory=list)


class CacheManager:
    """
    A simple, type-safe cache manager.

    Its sole responsibility is to hold the results of the database introspection.
    It has no discovery or generation logic.
    """

    def __init__(self, schemas: List[str]):
        self.schemas = schemas
        self.cache: Dict[str, SchemaCache] = {
            schema: SchemaCache() for schema in schemas
        }

    def get_schema(self, schema: str) -> SchemaCache | None:
        """Safely retrieves the cache for a given schema."""
        return self.cache.get(schema)

    def log_stats(self):
        """Prints a rich summary table of all cached objects."""
        console.rule("[bold]CacheManager Statistics", style="bold white")
        table = Table(header_style="bold", show_footer=True, box=None)
        table.add_column("Schema", style="cyan", no_wrap=True, footer="[bold]TOTAL[/]")
        table.add_column("Tables", style="blue", justify="right")
        table.add_column("Views", style="green", justify="right")
        table.add_column("Enums", style="yellow", justify="right")
        table.add_column("Functions", style="red", justify="right")
        table.add_column("Procedures", style="yellow", justify="right")
        table.add_column("Triggers", style="orange1", justify="right")
        table.add_column("Total", justify="right", style="bold")

        totals = {
            "tables": 0,
            "views": 0,
            "enums": 0,
            "functions": 0,
            "procedures": 0,
            "triggers": 0,
        }

        for schema_name, schema_cache in self.cache.items():
            counts = {
                "tables": len(schema_cache.tables),
                "views": len(schema_cache.views),
                "enums": len(schema_cache.enums),
                "functions": len(schema_cache.functions),
                "procedures": len(schema_cache.procedures),
                "triggers": len(schema_cache.triggers),
            }
            schema_total = sum(counts.values())
            table.add_row(
                schema_name,
                str(counts["tables"]),
                str(counts["views"]),
                str(counts["enums"]),
                str(counts["functions"]),
                str(counts["procedures"]),
                str(counts["triggers"]),
                str(schema_total),
            )
            for key in totals:
                totals[key] += counts[key]

        table.columns[1].footer = f"[blue]{totals['tables']}[/]"
        table.columns[2].footer = f"[green]{totals['views']}[/]"
        table.columns[3].footer = f"[magenta]{totals['enums']}[/]"
        table.columns[4].footer = f"[red]{totals['functions']}[/]"
        table.columns[5].footer = f"[yellow]{totals['procedures']}[/]"
        table.columns[6].footer = f"[orange1]{totals['triggers']}[/]"
        table.columns[7].footer = f"[bold]{sum(totals.values())}[/]"

        console.print(table)
        console.print()
