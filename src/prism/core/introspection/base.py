# src/prism/core/introspection/base.py
from abc import ABC, abstractmethod
from typing import Dict, List

from ..models.enums import EnumInfo
from ..models.functions import FunctionMetadata
from ..models.tables import TableMetadata


class IntrospectorABC(ABC):
    """Abstract Base Class for database introspection."""

    @abstractmethod
    def get_schemas(self) -> List[str]:
        """Returns a list of all user-defined schema names."""
        pass

    @abstractmethod
    def get_tables(self, schema: str) -> List[TableMetadata]:
        """Returns metadata for all tables and views in a given schema."""
        pass

    @abstractmethod
    def get_enums(self, schema: str) -> Dict[str, EnumInfo]:
        """Returns all enum type definitions in a given schema."""
        pass

    @abstractmethod
    def get_functions(self, schema: str) -> List[FunctionMetadata]:
        """Returns metadata for all functions and procedures in a given schema."""
        pass