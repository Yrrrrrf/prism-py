# src/prism/core/types/mapping.py
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, List, Optional, Type


class SqlTypeCategory(str, Enum):
    """Categories of SQL types for organized mapping."""

    NUMERIC = "numeric"
    STRING = "string"
    TEMPORAL = "temporal"
    BOOLEAN = "boolean"
    BINARY = "binary"
    JSON = "json"
    ARRAY = "array"
    ENUM = "enum"
    UUID = "uuid"
    OTHER = "other"


@dataclass(frozen=True)
class TypeMapping:
    """Defines a mapping from a SQL type pattern to a Python type."""

    sql_pattern: str
    python_type: Type
    category: SqlTypeCategory
    converter: Optional[Callable] = None


# This list is the core of the type mapping system. It can be expanded for other dialects.
SQL_TYPE_MAPPINGS: List[TypeMapping] = [
    # Numeric types
    TypeMapping(r"^smallint$|^int2$", int, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^integer$|^int$|^int4$", int, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^bigint$|^int8$", int, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^decimal.*$|^numeric.*$", "Decimal", SqlTypeCategory.NUMERIC),
    TypeMapping(r"^real$|^float4$", float, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^double precision$|^float8$", float, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^serial$|^bigserial$", int, SqlTypeCategory.NUMERIC),
    # String types
    TypeMapping(r"^character varying.*$|^varchar.*$", str, SqlTypeCategory.STRING),
    TypeMapping(r"^character.*$|^char.*$", str, SqlTypeCategory.STRING),
    TypeMapping(r"^text$", str, SqlTypeCategory.STRING),
    # Boolean type
    TypeMapping(r"^boolean$|^bool$", bool, SqlTypeCategory.BOOLEAN),
    # Date/Time types
    TypeMapping(r"^timestamp.*", "datetime", SqlTypeCategory.TEMPORAL),
    TypeMapping(r"^date$", "date", SqlTypeCategory.TEMPORAL),
    TypeMapping(r"^time.*", "time", SqlTypeCategory.TEMPORAL),
    TypeMapping(r"^interval$", "timedelta", SqlTypeCategory.TEMPORAL),
    # UUID type
    TypeMapping(r"^uuid$", "UUID", SqlTypeCategory.UUID),
    # JSON types
    TypeMapping(r"^json$", dict, SqlTypeCategory.JSON),
    TypeMapping(r"^jsonb$", dict, SqlTypeCategory.JSON),
    # Binary types
    TypeMapping(r"^bytea$", bytes, SqlTypeCategory.BINARY),
    # Other types
    TypeMapping(r"^.*$", Any, SqlTypeCategory.OTHER),  # Fallback
]
