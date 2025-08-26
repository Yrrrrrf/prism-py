# src/prism/core/types/utils.py
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import (
    Any,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

from prism.core.types.mapping import SQL_TYPE_MAPPINGS

T = TypeVar("T")

# Map string representations to actual types
_TYPE_MAP = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "bytes": bytes,
    "datetime": datetime,
    "date": date,
    "time": time,
    "timedelta": timedelta,
    "Decimal": Decimal,
    "UUID": UUID,
    "Any": Any,
    "dict": dict,
}

PY_TO_JSON_SCHEMA_TYPE = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
    datetime: "string",
    date: "string",
    time: "string",
    UUID: "string",
    Decimal: "number",
}


# Wrapper classes to handle complex types
class ArrayType(Generic[T]):
    def __init__(self, item_type: Type[T]):
        self.item_type: Type[T] = item_type

    def __repr__(self) -> str:
        return f"ArrayType[{self.item_type.__name__}]"


class JSONBType:
    def __repr__(self) -> str:
        return "JSONBType"


def make_optional(type_: Type) -> Type:
    if get_origin(type_) is Union and type(None) in get_args(type_):
        return type_
    return Optional[type_]


def parse_array_type(sql_type: str) -> ArrayType:
    base_type = sql_type.replace("[]", "").strip()
    element_type = get_python_type(base_type, nullable=False)
    # Unwrap Union/Optional for array items
    if get_origin(element_type) is Union:
        args = get_args(element_type)
        element_type = next((t for t in args if t is not type(None)), Any)
    return ArrayType(element_type)


def get_python_type(sql_type: str, nullable: bool = True) -> Type:
    sql_type_lower = sql_type.lower().strip()

    if sql_type_lower == "jsonb":
        return JSONBType()
    if sql_type_lower.endswith("[]"):
        array_type = parse_array_type(sql_type_lower)
        return make_optional(array_type) if nullable else array_type

    for mapping in SQL_TYPE_MAPPINGS:
        if re.match(mapping.sql_pattern, sql_type_lower):
            py_type = mapping.python_type
            if isinstance(py_type, str):
                py_type = _TYPE_MAP.get(py_type, Any)
            return make_optional(py_type) if nullable else py_type

    return Any


def string_to_list_converter(value: str) -> List[str]:
    """Converts a comma-separated string to a list of strings."""
    if not isinstance(value, str):
        return value  # Return as-is if not a string
    return [item.strip() for item in value.split(",")]
