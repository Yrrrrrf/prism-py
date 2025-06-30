# src/prism/core/models/tables.py
from dataclasses import dataclass, field
from typing import List, Optional

from prism.core.models.enums import EnumInfo


@dataclass(frozen=True)
class ColumnReference:
    """Represents a foreign key reference to another column."""

    schema: str
    table: str
    column: str


@dataclass(frozen=True)
class ColumnMetadata:
    """Internal representation of a database column's metadata."""

    name: str
    sql_type: str
    is_nullable: bool
    is_pk: bool
    default_value: Optional[str] = None
    comment: Optional[str] = None
    max_length: Optional[int] = None
    foreign_key: Optional[ColumnReference] = None
    enum_info: Optional[EnumInfo] = None  # Link to an enum if it's an enum type


@dataclass(frozen=True)
class TableMetadata:
    """Internal representation of a database table or view."""

    name: str
    schema: str
    columns: List[ColumnMetadata] = field(default_factory=list)
    primary_key_columns: List[str] = field(default_factory=list)
    is_view: bool = False
    comment: Optional[str] = None
