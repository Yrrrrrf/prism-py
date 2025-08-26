# src/prism/api/models.py
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

# These models define the public contract of the /dt/schemas endpoint.


class ApiColumnReference(BaseModel):
    """Represents a foreign key reference."""

    # =================== THE FIX ===================
    # Use `schema_name` as the field and alias it to "schema".
    schema_name: str = Field(alias="schema")
    # ===============================================
    table: str
    column: str


class ApiColumnMetadata(BaseModel):
    """Public representation of a database column."""

    name: str
    type: str
    nullable: bool
    is_pk: bool
    is_enum: bool
    references: Optional[ApiColumnReference] = None


class ApiTableMetadata(BaseModel):
    """Public representation of a database table or view."""

    name: str
    # =================== THE FIX ===================
    schema_name: str = Field(alias="schema")
    # ===============================================
    columns: List[ApiColumnMetadata] = Field(default_factory=list)


class ApiEnumMetadata(BaseModel):
    """Public representation of a database enum."""

    name: str
    # =================== THE FIX ===================
    schema_name: str = Field(alias="schema")
    # ===============================================
    values: List[str] = Field(default_factory=list)


class ApiFunctionParameter(BaseModel):
    """Public representation of a function/procedure parameter."""

    name: str
    type: str
    mode: str


class ApiFunctionMetadata(BaseModel):
    """Public representation of a database function or procedure."""

    name: str
    # =================== THE FIX ===================
    schema_name: str = Field(alias="schema")
    # ===============================================
    return_type: str
    parameters: List[ApiFunctionParameter] = Field(default_factory=list)


class ApiSchemaMetadata(BaseModel):
    """The main response model for the /dt/schemas endpoint. A complete map of a schema."""

    # This one is already correct, its field is `name`, not `schema`.
    name: str
    tables: Dict[str, ApiTableMetadata] = Field(default_factory=dict)
    views: Dict[str, ApiTableMetadata] = Field(default_factory=dict)
    enums: Dict[str, ApiEnumMetadata] = Field(default_factory=dict)
    functions: Dict[str, ApiFunctionMetadata] = Field(default_factory=dict)
    procedures: Dict[str, ApiFunctionMetadata] = Field(default_factory=dict)
    triggers: Dict[str, ApiFunctionMetadata] = Field(default_factory=dict)
