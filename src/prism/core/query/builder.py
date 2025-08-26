# src/prism/core/query/builder.py
import re
from typing import Any, Dict

from sqlalchemy.orm import Query

# Import all the necessary maps from the operators module
from .operators import (
    BOOLEAN_OPERATORS,
    CONVERTER_MAP,
    ORM_OPERATOR_MAP,
    SQL_OPERATOR_MAP,
)

# Regex to parse 'field[operator]' format from query keys.
QUERY_PARAM_REGEX = re.compile(r"(\w+)\[(\w+)\]")


class QueryBuilder:
    """Builds a filtered and sorted SQLAlchemy query from API request parameters."""

    def __init__(self, model: type, params: Dict[str, Any]):
        self.model = model
        self.params = params
        self.query: Query | None = None

    def build(self, initial_query: Query) -> Query:
        """Applies filters, sorting, and pagination to the initial ORM query."""
        self.query = initial_query

        self._apply_filters()
        self._apply_sorting()
        self._apply_pagination()

        return self.query

    def _apply_filters(self):
        """Parses and applies filters to the ORM query."""
        for key, value in self.params.items():
            if value is None:
                continue

            match = QUERY_PARAM_REGEX.match(key)
            if not match:
                continue

            field_name, operator = match.groups()

            # For ORM queries, use the ORM_OPERATOR_MAP
            if not hasattr(self.model, field_name) or operator not in ORM_OPERATOR_MAP:
                continue

            column = getattr(self.model, field_name)
            sqlalchemy_method_name = ORM_OPERATOR_MAP[operator]

            if operator in CONVERTER_MAP:
                value = CONVERTER_MAP[operator](value)

            if operator in BOOLEAN_OPERATORS:
                bool_value = str(value).lower() in ("true", "1", "t", "y", "yes")
                if bool_value:
                    self.query = self.query.filter(
                        getattr(column, sqlalchemy_method_name)(None)
                    )
                else:
                    self.query = self.query.filter(getattr(column, "is_not")(None))
                continue

            self.query = self.query.filter(
                getattr(column, sqlalchemy_method_name)(value)
            )

    def _apply_sorting(self):
        """Applies sorting to the ORM query."""
        order_by = self.params.get("order_by")
        if order_by and hasattr(self.model, order_by):
            column = getattr(self.model, order_by)
            order_dir = self.params.get("order_dir", "asc").lower()
            if order_dir == "desc":
                self.query = self.query.order_by(column.desc())
            else:
                self.query = self.query.order_by(column.asc())

    def _apply_pagination(self):
        """Applies pagination to the ORM query."""
        limit = self.params.get("limit")
        offset = self.params.get("offset")
        try:
            if limit is not None:
                self.query = self.query.limit(int(limit))
            if offset is not None:
                self.query = self.query.offset(int(offset))
        except (ValueError, TypeError):
            # Silently ignore invalid limit/offset values
            pass

    def build_clauses(self) -> tuple[str, str, str, str, dict]:
        """Builds raw SQL clauses and a parameter dictionary for use with views."""
        where_conditions = []
        params = {}

        # --- Filters ---
        for key, value in self.params.items():
            if value is None:
                continue
            match = QUERY_PARAM_REGEX.match(key)
            if not match:
                continue

            field_name, operator = match.groups()

            # For raw SQL, use the SQL_OPERATOR_MAP
            if not hasattr(self.model, field_name) or operator not in SQL_OPERATOR_MAP:
                continue

            sql_op = SQL_OPERATOR_MAP[operator]

            if operator in CONVERTER_MAP:
                value = CONVERTER_MAP[operator](value)

            param_name = f"{field_name}_{operator}"

            if operator in ("in", "notin"):
                if not value:
                    continue  # Avoid generating "IN ()" which is a syntax error
                # For IN clause, we need to expand the parameters for security
                in_params = [f":{param_name}_{i}" for i, _ in enumerate(value)]
                where_conditions.append(
                    f'"{field_name}" {sql_op} ({", ".join(in_params)})'
                )
                for i, v in enumerate(value):
                    params[f"{param_name}_{i}"] = v
            else:
                # Quote column names to handle reserved words
                where_conditions.append(f'"{field_name}" {sql_op} :{param_name}')
                params[param_name] = value

        where_clause = (
            f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        )

        # --- Sorting ---
        order_clause = ""
        order_by = self.params.get("order_by")
        if order_by and hasattr(self.model, order_by):
            order_dir = self.params.get("order_dir", "asc").lower()
            # Quote the column name to handle reserved words
            order_clause = (
                f'ORDER BY "{order_by}" {"DESC" if order_dir == "desc" else "ASC"}'
            )

        # --- Pagination ---
        limit_clause = ""
        offset_clause = ""
        try:
            if self.params.get("limit") is not None:
                limit_clause = f"LIMIT {int(self.params.get('limit'))}"
            if self.params.get("offset") is not None:
                offset_clause = f"OFFSET {int(self.params.get('offset'))}"
        except (ValueError, TypeError):
            # Silently ignore invalid limit/offset values
            pass

        return where_clause, order_clause, limit_clause, offset_clause, params
