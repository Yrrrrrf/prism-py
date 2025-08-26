# src/prism/core/models/functions.py
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List, Optional


class FunctionType(str, Enum):
    """Types of database functions."""

    SCALAR = "scalar"
    TABLE = "table"
    SET_RETURNING = "set"
    AGGREGATE = "aggregate"
    WINDOW = "window"


class ObjectType(str, Enum):
    """Types of database objects that are callable."""

    FUNCTION = "function"
    PROCEDURE = "procedure"
    TRIGGER = "trigger"


@dataclass(frozen=True)
class FunctionParameter:
    """Internal representation of a function/procedure parameter."""

    name: str
    type: str
    mode: str = "IN"
    has_default: bool = False
    default_value: Optional[Any] = None


@dataclass(frozen=True)
class FunctionMetadata:
    """Internal representation of a database function or procedure."""

    schema: str
    name: str
    type: FunctionType
    object_type: ObjectType
    parameters: List[FunctionParameter] = field(default_factory=list)
    return_type: Optional[str] = None
    is_strict: bool = False
    description: Optional[str] = None
