# src/prism/core/models/enums.py
from dataclasses import dataclass
from enum import Enum as PyEnum
from typing import List, Optional, Type


@dataclass(frozen=True)
class EnumInfo:
    """Internal representation of a database enum type."""

    name: str
    schema: str
    values: List[str]

    def to_python_enum(self) -> Type[PyEnum]:
        """Creates a Python ``str`` Enum class from this info.

        The ``str`` mixin makes members compare and bind as their plain values,
        which SQLAlchemy's reflected ``Enum`` columns require.
        """
        return PyEnum(self.name, {v: v for v in self.values}, type=str)
