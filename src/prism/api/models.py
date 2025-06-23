# src/prism/api/models.py
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

# These models define the public contract of the /dt/schemas endpoint.


class ApiColumnReference(BaseModel):
    """Represents a foreign key reference."""

    schema: str
    table: str
    column: str


class ApiColumnMetadata(BaseModel):
    """Public representation of a database column."""

    name: str
    type: str
    nullable: bool
    is_pk: bool
    is_enum: bool
    # A simple field doesn't need a default_factory for None
    references: Optional[ApiColumnReference] = None


class ApiTableMetadata(BaseModel):
    """Public representation of a database table or view."""

    name: str
    schema: str
    # Use default_factory for mutable list
    columns: List[ApiColumnMetadata] = Field(default_factory=list)


class ApiEnumMetadata(BaseModel):
    """Public representation of a database enum."""

    name: str
    schema: str
    values: List[str] = Field(default_factory=list)


class ApiFunctionParameter(BaseModel):
    """Public representation of a function/procedure parameter."""

    name: str
    type: str
    mode: str


class ApiFunctionMetadata(BaseModel):
    """Public representation of a database function or procedure."""

    name: str
    schema: str
    return_type: str
    parameters: List[ApiFunctionParameter] = Field(default_factory=list)


class ApiSchemaMetadata(BaseModel):
    """The main response model for the /dt/schemas endpoint. A complete map of a schema."""

    name: str
    # --- THIS IS THE FIX: Use default_factory for all mutable collections ---
    tables: Dict[str, ApiTableMetadata] = Field(default_factory=dict)
    views: Dict[str, ApiTableMetadata] = Field(default_factory=dict)
    enums: Dict[str, ApiEnumMetadata] = Field(default_factory=dict)
    functions: Dict[str, ApiFunctionMetadata] = Field(default_factory=dict)
    procedures: Dict[str, ApiFunctionMetadata] = Field(default_factory=dict)
    triggers: Dict[str, ApiFunctionMetadata] = Field(default_factory=dict)
