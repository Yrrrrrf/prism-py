# src/prism/core/query/builder.py
from sqlalchemy.orm import Query

from .operators import OPERATOR_MAP, LIST_OPERATORS

class QueryBuilder:
    """
    Builds a filtered and sorted SQLAlchemy query from API request parameters.
    """
    def __init__(self, model, params: dict):
        self.model = model
        self.params = params

    def build(self, initial_query: Query) -> Query:
        """
        Applies filters, sorting, and pagination to the initial query.
        """
        query = initial_query
        # TODO: Implement parsing of 'field[operator]=value' syntax
        # TODO: Implement sorting based on 'order_by' and 'order_dir'
        # TODO: Implement pagination based on 'limit' and 'offset'
        return query