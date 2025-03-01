# src/prism/api/base.py
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generic, List, Type, TypeVar

from fastapi import APIRouter
from sqlalchemy import Table

from prism.common.types import ArrayType, JSONBType, get_eq_type

# Type variables for generic typing
T = TypeVar("T")  # Response model
Q = TypeVar("Q")  # Query model


class RouteGenerator(ABC, Generic[T, Q]):
    """Base class for all route generators with common functionality."""

    def __init__(
        self,
        resource_name: str,
        router: APIRouter,
        db_dependency: Callable,
        schema: str,
        response_model: Type[T] = None,
        query_model: Type[Q] = None,
        table: Table = None,
        prefix: str = "",
    ):
        self.resource_name = resource_name
        self.router = router
        self.db_dependency = db_dependency
        self.schema = schema
        self.response_model = response_model
        self.query_model = query_model
        self.table = table
        self.prefix = prefix

    @abstractmethod
    def generate_routes(self) -> None:
        """Generate routes for this resource."""
        pass

    def get_route_path(self, operation: str = "") -> str:
        """Generate consistent route paths."""
        base_path = f"/{self.resource_name.lower()}"
        if operation:
            base_path = f"{base_path}/{operation}"
        return f"{self.prefix}{base_path}"

    def process_record_fields(
        self, record: Any, column_dict: Dict = None
    ) -> Dict[str, Any]:
        """Process record fields with proper type handling."""
        result = {}

        # If working with a SQLAlchemy model instance
        if hasattr(record, "__table__"):
            for column in record.__table__.columns:
                value = getattr(record, column.name)
                field_type = get_eq_type(str(column.type))

                # Process based on type
                if isinstance(field_type, JSONBType):
                    result[column.name] = self.process_jsonb_value(value)
                elif isinstance(field_type, ArrayType):
                    result[column.name] = self.process_array_value(
                        value, field_type.item_type
                    )
                else:
                    result[column.name] = value

        # If working with a result row mapping
        elif hasattr(record, "_mapping"):
            for column_name, value in dict(record._mapping).items():
                if column_dict and column_name in column_dict:
                    column = column_dict.get(column_name)
                    field_type = get_eq_type(str(column.type))

                    # Process based on type
                    if isinstance(field_type, JSONBType):
                        result[column_name] = self.process_jsonb_value(value)
                    elif isinstance(field_type, ArrayType):
                        result[column_name] = self.process_array_value(
                            value, field_type.item_type
                        )
                    else:
                        result[column_name] = value
                else:
                    result[column_name] = value

        # If working with a dictionary
        elif isinstance(record, dict):
            for column_name, value in record.items():
                if column_dict and column_name in column_dict:
                    column = column_dict.get(column_name)
                    field_type = get_eq_type(str(column.type))

                    # Process based on type
                    if isinstance(field_type, JSONBType):
                        result[column_name] = self.process_jsonb_value(value)
                    elif isinstance(field_type, ArrayType):
                        result[column_name] = self.process_array_value(
                            value, field_type.item_type
                        )
                    else:
                        result[column_name] = value
                else:
                    result[column_name] = value

        return result

    def process_jsonb_value(self, value: Any) -> Any:
        """Process JSONB values from database to Python objects."""
        import json

        if value is None:
            return None

        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        return value

    def process_array_value(self, value: Any, item_type: Type) -> List:
        """Process array values from database to Python lists."""
        if value is None:
            return []

        if isinstance(value, str):
            # Handle PostgreSQL array string format
            cleaned_value = value.strip("{}").split(",")
            return [
                self.convert_value(item.strip('"'), item_type)
                for item in cleaned_value
                if item.strip()
            ]

        if isinstance(value, list):
            return [
                self.convert_value(item, item_type)
                for item in value
                if item is not None
            ]

        return value

    def convert_value(self, value: Any, target_type: Type) -> Any:
        """Convert a value to the target type if possible."""
        if value is None:
            return None

        try:
            return target_type(value)
        except (ValueError, TypeError):
            return value

    def extract_filter_params(self, filters: Any) -> Dict[str, Any]:
        """Extract filter parameters excluding pagination/ordering fields."""
        filter_dict = {}

        if not hasattr(filters, "model_dump"):
            return filter_dict

        # Get all filter attributes
        all_attrs = filters.model_dump(exclude_unset=True)

        # Exclude standard query params
        standard_params = {"limit", "offset", "order_by", "order_dir"}

        # Keep only valid filter fields
        for key, value in all_attrs.items():
            if key not in standard_params and value is not None:
                filter_dict[key] = value

        return filter_dict

    def build_sql_query(self, filters: Any) -> tuple[str, Dict[str, Any]]:
        """Build SQL query with parameters."""
        # Base query
        query_parts = [f"SELECT * FROM {self.schema}.{self.resource_name}"]
        params = {}

        # Extract filter values
        filter_dict = self.extract_filter_params(filters)

        # Add WHERE clause if there are filters
        if filter_dict:
            conditions = []
            for field_name, value in filter_dict.items():
                if self.table and field_name in self.table.columns:
                    param_name = f"param_{field_name}"
                    conditions.append(f"{field_name} = :{param_name}")
                    params[param_name] = value

            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))

        # Add pagination if available
        if hasattr(filters, "limit") and filters.limit is not None:
            query_parts.append(f"LIMIT {filters.limit}")

        if hasattr(filters, "offset") and filters.offset is not None:
            query_parts.append(f"OFFSET {filters.offset}")

        # Add ordering if available
        if hasattr(filters, "order_by") and filters.order_by is not None:
            direction = (
                "DESC"
                if (hasattr(filters, "order_dir") and filters.order_dir == "desc")
                else "ASC"
            )
            query_parts.append(f"ORDER BY {filters.order_by} {direction}")

        return " ".join(query_parts), params
