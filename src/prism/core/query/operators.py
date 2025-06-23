# src/prism/core/query/operators.py

# Maps string operators from API query params to SQLAlchemy column methods.
# For example, `?age[gte]=18` will use the 'gte' key to call `Column.__ge__(18)`.
OPERATOR_MAP = {
    'eq': '__eq__',      # Equal
    'neq': '__ne__',     # Not Equal
    'gt': '__gt__',      # Greater Than
    'gte': '__ge__',     # Greater Than or Equal
    'lt': '__lt__',      # Less Than
    'lte': '__le__',     # Less Than or Equal
    'like': 'like',      # String LIKE
    'ilike': 'ilike',    # String ILIKE (case-insensitive)
    'in': 'in_',         # In a list of values
    'notin': 'not_in',   # Not in a list of values
    'isnull': 'is_',     # Is Null
}

# Operators that expect a list of values, typically comma-separated.
LIST_OPERATORS = {'in', 'notin'}