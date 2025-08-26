# src/prism/core/query/operators.py
from prism.core.types.utils import string_to_list_converter

# Maps string operators to SQLAlchemy ORM column methods. (Used by CrudGenerator)
ORM_OPERATOR_MAP = {
    "eq": "__eq__",
    "neq": "__ne__",
    "gt": "__gt__",
    "gte": "__ge__",
    "lt": "__lt__",
    "lte": "__le__",
    "like": "like",
    "ilike": "ilike",
    "in": "in_",
    "notin": "not_in",
    "isnull": "is_",
}

# Maps string operators to raw SQL syntax. (Used by ViewGenerator)
SQL_OPERATOR_MAP = {
    "eq": "=",
    "neq": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "like": "LIKE",
    "ilike": "ILIKE",
    "in": "IN",
    "notin": "NOT IN",
    "isnull": "IS NULL",
}

# Define which operators expect their value to be converted from a string to a list.
CONVERTER_MAP = {
    "in": string_to_list_converter,
    "notin": string_to_list_converter,
}

# Define which operators expect a boolean-like value ('true', 'false').
BOOLEAN_OPERATORS = {"isnull"}
