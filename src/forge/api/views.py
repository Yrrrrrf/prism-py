# src/forge/api/views.py
from typing import Dict, List, Any, Type, Callable, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import Table, text
from sqlalchemy.orm import Session

from forge.common.types import JSONBType, ArrayType, get_eq_type
from forge.core.logging import log, color_palette


class ViewOps:
    """Class to handle view operations with FastAPI routes."""
    
    def __init__(
        self,
        table: Table,
        query_model: Type[BaseModel],
        response_model: Type[BaseModel],
        router: APIRouter,
        db_dependency: Callable,
        schema: str,
        prefix: str = ""
    ):
        """Initialize ViewOps with view information."""
        self.table = table
        self.query_model = query_model
        self.response_model = response_model
        self.router = router
        self.db_dependency = db_dependency
        self.schema = schema
        self.prefix = prefix
        self.view_name = table.name
    
    def generate_route(self) -> None:
        """Generate route for querying the view."""
        @self.router.get(
            f"/{self.view_name}",
            response_model=List[self.response_model],
            summary=f"Get {self.view_name} view data",
            description=f"Retrieve data from {self.schema}.{self.view_name} view with optional filtering"
        )
        def get_view_data(
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends()
        ) -> List[self.response_model]:
            # Build and execute SQL query directly
            query, params = self._build_sql_query(filters)
            result = db.execute(text(query), params)
            
            # Process results with proper type conversion
            processed_records = self._process_results(result)
            
            return processed_records
    
    def _build_sql_query(self, filters: BaseModel) -> tuple[str, Dict[str, Any]]:
        """Build SQL query with parameters for view access."""
        # Start with base query
        query_parts = [f'SELECT * FROM {self.schema}.{self.view_name}']
        params = {}
        
        # Extract filter values
        filter_dict = {k: v for k, v in filters.model_dump(exclude_unset=True).items() 
                       if v is not None and k not in ('limit', 'offset', 'order_by', 'order_dir')}
        
        # Add WHERE clause if there are filters
        if filter_dict:
            conditions = []
            for field_name, value in filter_dict.items():
                # Check if column exists in the view
                if field_name in self.table.columns:
                    param_name = f"param_{field_name}"
                    conditions.append(f"{field_name} = :{param_name}")
                    params[param_name] = value
            
            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))
        
        # Add pagination if available
        if hasattr(filters, 'limit') and filters.limit is not None:
            query_parts.append(f"LIMIT {filters.limit}")
            
        if hasattr(filters, 'offset') and filters.offset is not None:
            query_parts.append(f"OFFSET {filters.offset}")
        
        # Add ordering if available
        if hasattr(filters, 'order_by') and filters.order_by is not None:
            direction = "DESC" if (hasattr(filters, 'order_dir') and 
                                   filters.order_dir == "desc") else "ASC"
            query_parts.append(f"ORDER BY {filters.order_by} {direction}")
        
        return " ".join(query_parts), params
    
    def _process_results(self, result) -> List[BaseModel]:
        """Process query results with proper type conversion."""
        import json
        
        processed_records = []
        for row in result:
            # Convert row to dictionary
            record_dict = dict(row._mapping)
            processed_record = {}
            
            # Process each column with type conversion
            for column_name, value in record_dict.items():
                column = self.table.columns.get(column_name)
                if column:
                    field_type = get_eq_type(str(column.type))
                    
                    # Handle JSONB fields
                    if isinstance(field_type, JSONBType):
                        if value is not None:
                            if isinstance(value, str):
                                try:
                                    processed_record[column_name] = json.loads(value)
                                except json.JSONDecodeError:
                                    processed_record[column_name] = value
                            else:
                                processed_record[column_name] = value
                        else:
                            processed_record[column_name] = None
                    
                    # Handle array fields
                    elif isinstance(field_type, ArrayType):
                        if value is not None:
                            if isinstance(value, str):
                                # Convert PostgreSQL array string format
                                cleaned_value = value.strip('{}').split(',')
                                processed_record[column_name] = [
                                    field_type.item_type(item.strip('"')) 
                                    for item in cleaned_value 
                                    if item.strip()
                                ]
                            elif isinstance(value, list):
                                processed_record[column_name] = value
                            else:
                                processed_record[column_name] = value
                        else:
                            processed_record[column_name] = []
                    
                    # Handle regular fields
                    else:
                        processed_record[column_name] = value
                else:
                    # Column not found in table definition, pass through as is
                    processed_record[column_name] = value
            
            # Validate record with response model
            try:
                validated_record = self.response_model.model_validate(processed_record)
                processed_records.append(validated_record)
            except Exception as e:
                log.error(f"Validation error for record in {self.view_name}: {str(e)}")
                continue
        
        return processed_records