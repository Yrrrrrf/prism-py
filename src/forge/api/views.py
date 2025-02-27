# src/forge/api/views.py
from forge.api.router import RouteGenerator
from typing import Any, Callable, Dict, List, Optional, Type
from fastapi import APIRouter, Depends
from sqlalchemy import Table, text
from sqlalchemy.orm import Session
from pydantic import BaseModel


# src/forge/api/views.py (partial update)
class ViewGenerator(RouteGenerator):
    """Generator for view routes."""

    def __init__(
        self,
        table: Table,
        query_model: Type[BaseModel],
        response_model: Type[BaseModel],
        router: APIRouter,
        db_dependency: Callable,
        schema: str,
        prefix: str = "",
        enhanced_filtering: bool = True,  # Support enhanced filtering option
    ):
        super().__init__(
            resource_name=table.name,
            router=router,
            db_dependency=db_dependency,
            schema=schema,
            response_model=response_model,
            query_model=query_model,
            table=table,
            prefix=prefix,
        )
        self.enhanced_filtering = enhanced_filtering

    def generate_routes(self):
        """Generate view route."""

        @self.router.get(
            self.get_route_path(),
            response_model=List[self.response_model],
            summary=f"Get {self.resource_name} view data",
            description=f"Retrieve data from {self.schema}.{self.resource_name} view with optional filtering",
        )
        def get_view_data(
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),
        ) -> List[self.response_model]:
            # Build query parts and parameters
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

            # Add enhanced filtering if enabled
            if self.enhanced_filtering:
                # Add pagination if available
                if hasattr(filters, "limit") and filters.limit is not None:
                    query_parts.append(f"LIMIT {filters.limit}")

                if hasattr(filters, "offset") and filters.offset is not None:
                    query_parts.append(f"OFFSET {filters.offset}")

                # Add ordering if available
                if hasattr(filters, "order_by") and filters.order_by is not None:
                    direction = (
                        "DESC"
                        if (
                            hasattr(filters, "order_dir")
                            and filters.order_dir == "desc"
                        )
                        else "ASC"
                    )
                    query_parts.append(f"ORDER BY {filters.order_by} {direction}")

            # Execute SQL query
            query = " ".join(query_parts)
            result = db.execute(text(query), params)

            # Process results with column dictionary for type conversion
            column_dict = {col.name: col for col in self.table.columns}

            # Process and validate records
            processed_records = []
            for row in result:
                processed_record = self.process_record_fields(row, column_dict)
                try:
                    validated_record = self.response_model.model_validate(
                        processed_record
                    )
                    processed_records.append(validated_record)
                except Exception as e:
                    # Skip invalid records
                    pass

            return processed_records
