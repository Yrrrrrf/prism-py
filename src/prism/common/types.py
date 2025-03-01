# This is a proposed reorganization to avoid duplicated type definitions

# ==== src/prism/common/types.py ====
# Keep all core data models and type logic here

"""Type definitions and mapping utilities used across the prism-py framework."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, create_model

# ===== Type Aliases =====
SqlType = str  # SQL type string (e.g., "varchar(255)")
PythonType = Type  # Python type (e.g., str, int)
ModelType = Type[BaseModel]  # Pydantic model type
JsonData = Union[Dict[str, Any], List[Dict[str, Any]]]  # JSON-like data
TypeMapper = Callable[[SqlType, Any, bool], Type]  # Type mapping function

# Type variable for generic model creation
T = TypeVar("T")


# ===== Base Models =====
class PrismBaseModel(BaseModel):
    """Base model for all prism-generated models with optimal settings."""

    model_config = ConfigDict(
        from_attributes=True,  # Allow ORM mode
        populate_by_name=True,  # Support alias population
        arbitrary_types_allowed=True,  # Allow non-pydantic types
        validate_assignment=True,  # Validate on attribute assignment
        extra="ignore",  # Ignore extra fields
        str_strip_whitespace=True,  # Strip whitespace from strings
    )


# ===== Enum, Function and Metadata Types =====
@dataclass
class EnumInfo:
    """Store information about database enums."""

    name: str
    values: List[str]
    python_enum: Optional[Type[Enum]] = None
    schema: Optional[str] = None

    def create_enum(self) -> Type[Enum]:
        """Create a Python Enum from the enum information."""
        if not self.python_enum:
            self.python_enum = Enum(self.name, {v: v for v in self.values})
        return self.python_enum


@dataclass
class FunctionParameter:
    """Function parameter metadata."""

    name: str
    type: str
    mode: str = "IN"  # IN, OUT, INOUT, VARIADIC
    has_default: bool = False
    default_value: Optional[Any] = None


class FunctionType(str, Enum):
    """Types of database functions."""

    SCALAR = "scalar"
    TABLE = "table"
    SET_RETURNING = "set"
    AGGREGATE = "aggregate"
    WINDOW = "window"


class ObjectType(str, Enum):
    """Types of database objects."""

    FUNCTION = "function"
    PROCEDURE = "procedure"
    TRIGGER = "trigger"
    AGGREGATE = "aggregate"
    WINDOW = "window"


@dataclass
class FunctionMetadata:
    """Metadata for database functions."""

    schema: str
    name: str
    type: FunctionType
    object_type: ObjectType
    parameters: List[FunctionParameter] = field(default_factory=list)
    return_type: Optional[str] = None
    is_strict: bool = False
    description: Optional[str] = None


@dataclass
class ColumnReference:
    """Reference to another column (for foreign keys)."""

    schema: str
    table: str
    column: str


# ===== API Response Models =====
# These are the Pydantic models used specifically for API responses


class ApiColumnReference(PrismBaseModel):
    """Reference to another database column (for foreign keys)."""

    schema_name: str = Field(alias="schema")
    table: str
    column: str


class ApiColumnMetadata(PrismBaseModel):
    """Column metadata for table or view."""

    name: str
    type: str
    nullable: bool
    is_primary_key: Optional[bool] = Field(default=None, alias="is_pk")
    is_enum: Optional[bool] = None
    references: Optional[ApiColumnReference] = None


class ApiEntityMetadata(PrismBaseModel):
    """Base class for database entity metadata."""

    name: str
    schema_name: str = Field(alias="schema")


class ApiTableMetadata(ApiEntityMetadata):
    """Table structure metadata."""

    columns: List[ApiColumnMetadata] = []


class ApiEnumValue(PrismBaseModel):
    """Enum value information."""

    name: str
    value: str


class ApiEnumMetadata(ApiEntityMetadata):
    """Enum type metadata."""

    values: List[str] = []


class ApiFunctionParameter(PrismBaseModel):
    """Function parameter metadata."""

    name: str
    type: str
    mode: str = "IN"  # IN, OUT, INOUT, VARIADIC
    has_default: bool = False
    default_value: Optional[str] = None


class ApiReturnColumn(PrismBaseModel):
    """Return column for table-returning functions."""

    name: str
    type: str


class ApiFunctionMetadata(ApiEntityMetadata):
    """Database function metadata."""

    type: str  # scalar, table, set, etc.
    object_type: str  # function, procedure, trigger
    description: Optional[str] = None
    parameters: List[ApiFunctionParameter] = []
    return_type: Optional[str] = None
    return_columns: Optional[List[ApiReturnColumn]] = None
    is_strict: bool = False


class ApiTriggerEvent(PrismBaseModel):
    """Trigger event information."""

    timing: str  # BEFORE, AFTER, INSTEAD OF
    events: List[str]  # INSERT, UPDATE, DELETE, TRUNCATE
    table_schema: str
    table_name: str


class ApiTriggerMetadata(ApiFunctionMetadata):
    """Trigger metadata extending function metadata."""

    trigger_data: ApiTriggerEvent


class ApiSchemaMetadata(PrismBaseModel):
    """Complete schema metadata including all database objects."""

    name: str
    tables: Dict[str, ApiTableMetadata] = {}
    views: Dict[str, ApiTableMetadata] = {}
    enums: Dict[str, ApiEnumMetadata] = {}
    functions: Dict[str, ApiFunctionMetadata] = {}
    procedures: Dict[str, ApiFunctionMetadata] = {}
    triggers: Dict[str, ApiTriggerMetadata] = {}


# ===== Conversion Functions =====
# These functions convert between internal data structures and API models


def to_api_function_parameter(param: FunctionParameter) -> ApiFunctionParameter:
    """Convert internal function parameter to API response model."""
    return ApiFunctionParameter(
        name=param.name,
        type=param.type,
        mode=param.mode,
        has_default=param.has_default,
        default_value=str(param.default_value)
        if param.default_value is not None
        else None,
    )


def to_api_function_metadata(fn: FunctionMetadata) -> ApiFunctionMetadata:
    """Convert internal function metadata to API response model."""
    return ApiFunctionMetadata(
        name=fn.name,
        schema=fn.schema,
        type=str(fn.type),
        object_type=str(fn.object_type),
        description=fn.description,
        parameters=[to_api_function_parameter(p) for p in fn.parameters],
        return_type=fn.return_type,
        is_strict=fn.is_strict,
    )


def to_api_column_reference(ref: ColumnReference) -> ApiColumnReference:
    """Convert internal column reference to API response model."""
    return ApiColumnReference(
        schema=ref.schema,
        table=ref.table,
        column=ref.column,
    )


# ===== SQL Type Mapping =====
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
    NETWORK = "network"
    GEOMETRIC = "geometric"
    OTHER = "other"


@dataclass
class TypeMapping:
    """Mapping between SQL and Python types with metadata."""

    sql_pattern: str  # Regex pattern to match SQL type
    python_type: Type  # Corresponding Python type
    category: SqlTypeCategory  # Type category
    converter: Optional[Callable] = None  # Optional conversion function


class ArrayType(Generic[T]):
    """
    Wrapper class for array types with type information.

    This class represents a PostgreSQL array type with known item type.
    """

    def __init__(self, item_type: Type[T]):
        self.item_type: Type[T] = item_type

    def __call__(self) -> List[T]:
        """Initialize an empty list when called."""
        return []

    def __repr__(self) -> str:
        return f"ArrayType[{self.item_type.__name__}]"


class JSONBType:
    """
    Wrapper class for JSONB types with dynamic model creation.

    Automatically creates appropriate Pydantic models based on sample data.
    """

    def __init__(self, sample_data: Any = None):
        self.sample_data = sample_data
        self._model_cache: Dict[str, ModelType] = {}

    def get_model(self, name: str) -> ModelType:
        """
        Get or create a Pydantic model matching the JSONB structure.

        Args:
            name: The name to use for the generated model

        Returns:
            A Pydantic model class for the JSONB data
        """
        if name not in self._model_cache:
            if self.sample_data is None:
                return dict
            try:
                self._model_cache[name] = create_dynamic_model(self.sample_data, name)
            except Exception:
                # Fallback to dict if model creation fails
                return dict
        return self._model_cache[name]

    def __repr__(self) -> str:
        return "JSONBType"


# ===== Type Mapping Registry =====
# Comprehensive mapping for SQL types to Python types
SQL_TYPE_MAPPINGS: List[TypeMapping] = [
    # Numeric types
    TypeMapping(r"^smallint$", int, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^integer$", int, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^bigint$", int, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^decimal(\(\d+,\s*\d+\))?$", Decimal, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^numeric(\(\d+,\s*\d+\))?$", Decimal, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^real$", float, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^double precision$", float, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^serial$", int, SqlTypeCategory.NUMERIC),
    TypeMapping(r"^bigserial$", int, SqlTypeCategory.NUMERIC),
    # String types
    TypeMapping(r"^varchar(\(\d+\))?$", str, SqlTypeCategory.STRING),
    TypeMapping(r"^character varying(\(\d+\))?$", str, SqlTypeCategory.STRING),
    TypeMapping(r"^char(\(\d+\))?$", str, SqlTypeCategory.STRING),
    TypeMapping(r"^text$", str, SqlTypeCategory.STRING),
    # Boolean type
    TypeMapping(r"^boolean$", bool, SqlTypeCategory.BOOLEAN),
    TypeMapping(r"^bool$", bool, SqlTypeCategory.BOOLEAN),
    # Date/Time types
    TypeMapping(
        r"^timestamp(\(\d+\))?( with(out)? time zone)?$",
        datetime,
        SqlTypeCategory.TEMPORAL,
    ),
    TypeMapping(r"^date$", date, SqlTypeCategory.TEMPORAL),
    TypeMapping(
        r"^time(\(\d+\))?( with(out)? time zone)?$", time, SqlTypeCategory.TEMPORAL
    ),
    TypeMapping(r"^interval$", timedelta, SqlTypeCategory.TEMPORAL),
    # UUID type
    TypeMapping(r"^uuid$", UUID, SqlTypeCategory.UUID),
    # JSON types
    TypeMapping(r"^json$", dict, SqlTypeCategory.JSON),
    TypeMapping(r"^jsonb$", dict, SqlTypeCategory.JSON),
    # Binary types
    TypeMapping(r"^bytea$", bytes, SqlTypeCategory.BINARY),
    TypeMapping(r"^bit(\(\d+\))?$", bool, SqlTypeCategory.BINARY),
    TypeMapping(r"^bit varying(\(\d+\))?$", str, SqlTypeCategory.BINARY),
    # Network types
    TypeMapping(r"^inet$", str, SqlTypeCategory.NETWORK),
    TypeMapping(r"^cidr$", str, SqlTypeCategory.NETWORK),
    TypeMapping(r"^macaddr$", str, SqlTypeCategory.NETWORK),
    TypeMapping(r"^macaddr8$", str, SqlTypeCategory.NETWORK),
    # Geometric types
    TypeMapping(r"^point$", str, SqlTypeCategory.GEOMETRIC),
    TypeMapping(r"^line$", str, SqlTypeCategory.GEOMETRIC),
    TypeMapping(r"^lseg$", str, SqlTypeCategory.GEOMETRIC),
    TypeMapping(r"^box$", str, SqlTypeCategory.GEOMETRIC),
    TypeMapping(r"^path$", str, SqlTypeCategory.GEOMETRIC),
    TypeMapping(r"^polygon$", str, SqlTypeCategory.GEOMETRIC),
    TypeMapping(r"^circle$", str, SqlTypeCategory.GEOMETRIC),
    # Money type (represented as string to avoid precision issues)
    TypeMapping(r"^money$", str, SqlTypeCategory.NUMERIC),
    # Text search types
    TypeMapping(r"^tsvector$", str, SqlTypeCategory.OTHER),
    TypeMapping(r"^tsquery$", str, SqlTypeCategory.OTHER),
    # XML type
    TypeMapping(r"^xml$", str, SqlTypeCategory.OTHER),
    # Enum types (handled specially)
    TypeMapping(r"^enum$", str, SqlTypeCategory.ENUM),
    # User-defined types (fallback)
    TypeMapping(r"^.*$", Any, SqlTypeCategory.OTHER),
]


# ===== CRUD/API Helper Models =====
class QueryParams(PrismBaseModel):
    """Base model for query parameters."""

    limit: Optional[int] = Field(
        default=100, description="Maximum number of records to return"
    )
    offset: Optional[int] = Field(default=0, description="Number of records to skip")
    order_by: Optional[str] = Field(default=None, description="Field to order by")
    order_dir: Optional[str] = Field(
        default="asc", description="Order direction (asc or desc)"
    )


# ===== Core Type Functions =====
def infer_type(value: Any) -> Type:
    """
    Infer Python type from a value.

    Args:
        value: The value to analyze

    Returns:
        The inferred Python type
    """
    match value:
        case None:
            return Any
        case bool():
            return bool
        case int():
            return int
        case float():
            return float
        case datetime():
            return datetime
        case date():
            return date
        case UUID():
            return UUID
        case str():
            # Try to parse as UUID first
            try:
                UUID(value)
                return UUID
            except (ValueError, AttributeError):
                return str
        case list() if value and all(isinstance(x, dict) for x in value):
            return List[dict]
        case list() if value:
            # Try to infer uniform list type
            item_type = infer_type(value[0])
            if all(isinstance(x, (type(None), item_type.__class__)) for x in value):
                return List[item_type]
            return List[Any]
        case list():
            return List[Any]
        case dict():
            return dict
        case _:
            return type(value)


def create_dynamic_model(json_data: JsonData, model_name: str) -> ModelType:
    """
    Create a Pydantic model dynamically from JSON data.

    This function recursively analyzes JSON structures and creates
    appropriate Pydantic models with proper field types.

    Args:
        json_data: JSON data to analyze
        model_name: Name for the generated model

    Returns:
        A Pydantic model class with appropriate fields

    Raises:
        ValueError: If the JSON structure can't be converted to a model
    """
    # Determine template to use for field extraction
    template: Dict[str, Any]

    if isinstance(json_data, list) and json_data:
        # Use first item as template for list of objects
        template = json_data[0] if isinstance(json_data[0], dict) else {}
    elif isinstance(json_data, dict):
        template = json_data
    else:
        raise ValueError(f"Cannot create model from {type(json_data).__name__}")

    # Generate fields for the model
    fields: Dict[str, Tuple[Type, Any]] = {}

    for key, value in template.items():
        # Handle field based on value type
        if isinstance(value, dict):
            # Nested object
            nested_model = create_dynamic_model(value, f"{model_name}_{key}")
            fields[key] = (Optional[nested_model], Field(default=None))

        elif isinstance(value, list) and value and isinstance(value[0], dict):
            # List of objects
            item_model = create_dynamic_model(value[0], f"{model_name}_{key}_item")
            fields[key] = (Optional[List[item_model]], Field(default_factory=list))

        elif isinstance(value, list) and value:
            # List of simple types
            item_type = infer_type(value[0])
            fields[key] = (Optional[List[item_type]], Field(default_factory=list))

        else:
            # Simple type
            inferred_type = infer_type(value)
            fields[key] = (Optional[inferred_type], Field(default=None))

    # Create model with generated fields
    return create_model(model_name, __base__=PrismBaseModel, **fields)


def parse_array_type(sql_type: str) -> Type:
    """
    Parse PostgreSQL array type into Python List type.

    Args:
        sql_type: SQL array type string (e.g., "integer[]")

    Returns:
        Appropriate Python List type
    """
    # Extract base type from array notation
    base_type = sql_type.replace("[]", "").strip()

    # Map base type to Python type
    element_type = get_eq_type(base_type, nullable=False)

    # Unwrap Union/Optional for array items
    if get_origin(element_type) is Union:
        args = get_args(element_type)
        # Find the non-None type in the Union
        element_type = next((t for t in args if t is not type(None)), Any)

    # Create appropriate array type
    return ArrayType(element_type)


def make_optional(type_: Type) -> Type:
    """
    Make a type optional if it isn't already.

    Args:
        type_: Type to make optional

    Returns:
        Optional version of the type
    """
    # If already Optional/Union with None, return as is
    if get_origin(type_) is Union and type(None) in get_args(type_):
        return type_

    # Otherwise, make it Optional
    return Optional[type_]


def get_eq_type(sql_type: str, sample_data: Any = None, nullable: bool = True) -> Type:
    """
    Map SQL type to equivalent Python/Pydantic type.

    This function handles special cases like JSONB, arrays, and enums,
    and properly manages nullable types.

    Args:
        sql_type: SQL type string
        sample_data: Optional sample data for JSONB inference
        nullable: Whether the field is nullable

    Returns:
        Appropriate Python type
    """
    # Normalize SQL type
    sql_type_lower = sql_type.lower().strip()

    # Handle special cases first
    if sql_type_lower == "jsonb":
        # JSONB with optional sample data
        return JSONBType(sample_data)

    if sql_type_lower.endswith("[]"):
        # Array type
        array_type = parse_array_type(sql_type_lower)
        return make_optional(array_type) if nullable else array_type

    # Try to match against known types
    for mapping in SQL_TYPE_MAPPINGS:
        if re.match(mapping.sql_pattern, sql_type_lower):
            python_type = mapping.python_type
            return make_optional(python_type) if nullable else python_type

    # Default fallback
    return Any


def create_query_params_model(
    model: Type[BaseModel], table_columns: List[Any]
) -> Type[BaseModel]:
    """
    Create a query parameters model for filtering table data.

    Args:
        model: The base Pydantic model for the table
        table_columns: SQLAlchemy columns from the table

    Returns:
        A Pydantic model for query parameters
    """
    query_fields = {}

    # Add standard pagination fields
    query_fields["limit"] = (Optional[int], Field(default=100))
    query_fields["offset"] = (Optional[int], Field(default=0))
    query_fields["order_by"] = (Optional[str], Field(default=None))
    query_fields["order_dir"] = (Optional[str], Field(default="asc"))

    # Add fields for each column in the table
    for column in table_columns:
        field_type = get_eq_type(str(column.type))

        # Handle special types appropriately
        if isinstance(field_type, (JSONBType, ArrayType)):
            query_fields[column.name] = (Optional[str], Field(default=None))
        else:
            # Make all filter fields optional
            if get_origin(field_type) is Union:
                query_fields[column.name] = (field_type, Field(default=None))
            else:
                query_fields[column.name] = (Optional[field_type], Field(default=None))

    return create_model(
        f"{model.__name__}QueryParams", **query_fields, __base__=QueryParams
    )


# ===== Validation Functions =====
def validate_type(value: Any, expected_type: Type) -> bool:
    """
    Validate a value against an expected type.

    Args:
        value: The value to validate
        expected_type: The expected type

    Returns:
        True if value matches the expected type, False otherwise
    """
    # Handle None with Optional types
    if value is None:
        if get_origin(expected_type) is Union and type(None) in get_args(expected_type):
            return True
        return False

    # Handle ArrayType
    if isinstance(expected_type, ArrayType):
        if not isinstance(value, list):
            return False
        return all(isinstance(item, expected_type.item_type) for item in value)

    # Handle JSONBType
    if isinstance(expected_type, JSONBType):
        return isinstance(value, (dict, list))

    # Handle standard types
    origin = get_origin(expected_type)
    if origin is Union:
        return any(validate_type(value, arg) for arg in get_args(expected_type))
    elif origin is list or origin is List:
        if not isinstance(value, list):
            return False
        item_type = get_args(expected_type)[0]
        return all(validate_type(item, item_type) for item in value)

    # Direct type check
    return isinstance(value, expected_type)


def convert_value(value: Any, target_type: Type) -> Any:
    """
    Convert a value to the target type if possible.

    Args:
        value: The value to convert
        target_type: The target type

    Returns:
        Converted value

    Raises:
        ValueError: If conversion fails
    """
    if value is None:
        return None

    # Unwrap Optional types
    if get_origin(target_type) is Union:
        args = get_args(target_type)
        # Find the non-None type in the Union
        non_none_type = next((t for t in args if t is not type(None)), Any)
        return convert_value(value, non_none_type)

    # Handle special conversions
    match target_type:
        case UUID if isinstance(value, str):
            return UUID(value)
        case datetime if isinstance(value, str):
            return datetime.fromisoformat(value)
        case date if isinstance(value, str):
            return date.fromisoformat(value)
        case Decimal if isinstance(value, (int, float, str)):
            return Decimal(str(value))

    # Try direct conversion
    try:
        return target_type(value)
    except (ValueError, TypeError):
        raise ValueError(f"Cannot convert {value} to {target_type}")


# ===== Processing Functions =====
def process_jsonb_value(value: Any) -> Any:
    """Process JSONB values from database to Python objects."""
    if value is None:
        return None

    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    return value


def process_array_value(value: Any, item_type: Type) -> List:
    """Process array values from database to Python lists."""
    if value is None:
        return []

    if isinstance(value, str):
        # Handle PostgreSQL array string format
        cleaned_value = value.strip("{}").split(",")
        return [
            convert_value(item.strip('"'), item_type)
            for item in cleaned_value
            if item.strip()
        ]

    if isinstance(value, list):
        return [convert_value(item, item_type) for item in value if item is not None]

    return value
