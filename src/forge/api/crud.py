# src/forge/api/crud.py
"""CRUD operations for database tables with FastAPI routes."""

from typing import Any, Callable, Dict, List, Optional, Type, Union
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import Table
from pydantic import BaseModel

from forge.common.types import (
    ForgeBaseModel,
    JSONBType,
    ArrayType,
    get_eq_type,
    create_query_params_model,
    process_jsonb_value,
    process_array_value,
)


class CrudOps:
    """Class to handle CRUD operations with FastAPI routes."""

    def __init__(
        self,
        table: Table,
        pydantic_model: Type[BaseModel],
        sqlalchemy_model: Type[Any],
        router: APIRouter,
        db_dependency: Callable,
        prefix: str = "",
    ):
        """Initialize CRUD handler with common parameters."""
        self.table = table
        self.pydantic_model = pydantic_model
        self.sqlalchemy_model = sqlalchemy_model
        self.router = router
        self.db_dependency = db_dependency
        self.prefix = prefix

        # Create query params model using the enhanced utility function
        self.query_params = create_query_params_model(pydantic_model, table.columns)

    def _get_route_path(self, operation: str = "") -> str:
        """Generate route path with optional prefix."""
        base_path = f"/{self.table.name.lower()}"
        if operation:
            base_path = f"{base_path}/{operation}"
        return f"{self.prefix}{base_path}"

    def create(self) -> None:
        """Add CREATE route."""

        @self.router.post(
            self._get_route_path(),
            response_model=self.pydantic_model,
            summary=f"Create {self.table.name}",
            description=f"Create a new {self.table.name} record",
        )
        def create_resource(
            resource: self.pydantic_model, db: Session = Depends(self.db_dependency)
        ) -> self.pydantic_model:
            # Extract data excluding unset values
            data = resource.model_dump(exclude_unset=True)

            try:
                # Create new record instance
                db_resource = self.sqlalchemy_model(**data)
                db.add(db_resource)
                db.commit()
                db.refresh(db_resource)

                # Prepare result data with proper field conversions
                result_dict = self._process_record_fields(db_resource)
                return self.pydantic_model(**result_dict)
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=400, detail=f"Creation failed: {str(e)}"
                )

    def read(self) -> None:
        """Add READ route with enhanced JSONB handling."""

        @self.router.get(
            self._get_route_path(),
            response_model=List[self.pydantic_model],
            summary=f"Get {self.table.name} resources",
            description=f"Retrieve {self.table.name} records with optional filtering",
        )
        def read_resources(
            db: Session = Depends(self.db_dependency),
            filters: self.query_params = Depends(),
        ) -> List[self.pydantic_model]:
            # Build query with filters
            query = self._build_filtered_query(db, filters)

            # Apply pagination if provided
            if filters.limit:
                query = query.limit(filters.limit)
            if filters.offset:
                query = query.offset(filters.offset)

            # Apply ordering if provided
            if filters.order_by:
                order_column = getattr(self.sqlalchemy_model, filters.order_by, None)
                if order_column:
                    query = query.order_by(
                        order_column.desc()
                        if filters.order_dir == "desc"
                        else order_column
                    )

            # Execute query
            resources = query.all()

            # Process and validate results
            return [
                self.pydantic_model.model_validate(
                    self._process_record_fields(resource)
                )
                for resource in resources
            ]

    def update(self) -> None:
        """Add UPDATE route."""

        @self.router.put(
            self._get_route_path(),
            response_model=Dict[str, Any],
            summary=f"Update {self.table.name}",
            description=f"Update {self.table.name} records that match the filter criteria",
        )
        def update_resource(
            resource: self.pydantic_model,
            db: Session = Depends(self.db_dependency),
            filters: self.query_params = Depends(),
        ) -> Dict[str, Any]:
            update_data = resource.model_dump(exclude_unset=True)
            filters_dict = self._extract_filter_params(filters)

            if not filters_dict:
                raise HTTPException(status_code=400, detail="No filters provided")

            try:
                # Build query with filters
                query = self._build_filtered_query(db, filters)

                # Get records before update
                resources_before = query.all()
                if not resources_before:
                    raise HTTPException(
                        status_code=404, detail="No matching resources found"
                    )

                # Store old data for response
                old_data = [
                    self.pydantic_model.model_validate(
                        self._process_record_fields(resource)
                    )
                    for resource in resources_before
                ]

                # Perform update
                updated_count = query.update(update_data)
                db.commit()

                # Get updated records
                resources_after = query.all()
                updated_data = [
                    self.pydantic_model.model_validate(
                        self._process_record_fields(resource)
                    )
                    for resource in resources_after
                ]

                return {
                    "updated_count": updated_count,
                    "old_data": [d.model_dump() for d in old_data],
                    "updated_data": [d.model_dump() for d in updated_data],
                }
            except Exception as e:
                db.rollback()
                raise HTTPException(status_code=400, detail=f"Update failed: {str(e)}")

    def delete(self) -> None:
        """Add DELETE route."""

        @self.router.delete(
            self._get_route_path(),
            response_model=Dict[str, Any],
            summary=f"Delete {self.table.name}",
            description=f"Delete {self.table.name} records that match the filter criteria",
        )
        def delete_resource(
            db: Session = Depends(self.db_dependency),
            filters: self.query_params = Depends(),
        ) -> Dict[str, Any]:
            filters_dict = self._extract_filter_params(filters)

            if not filters_dict:
                raise HTTPException(status_code=400, detail="No filters provided")

            try:
                # Build query with filters
                query = self._build_filtered_query(db, filters)

                # Get resources before deletion
                to_delete = query.all()
                if not to_delete:
                    return {"message": "No resources found matching the criteria"}

                # Store deleted data for response
                deleted_resources = [
                    self.pydantic_model.model_validate(
                        self._process_record_fields(resource)
                    ).model_dump()
                    for resource in to_delete
                ]

                # Perform deletion
                deleted_count = query.delete(synchronize_session=False)
                db.commit()

                return {
                    "message": f"{deleted_count} resource(s) deleted successfully",
                    "deleted_resources": deleted_resources,
                }
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=400, detail=f"Deletion failed: {str(e)}"
                )

    def generate_all(self) -> None:
        """Generate all CRUD routes."""
        self.create()
        self.read()
        self.update()
        self.delete()

    # ===== Helper Methods =====

    def _extract_filter_params(self, filters: Any) -> Dict[str, Any]:
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

    def _build_filtered_query(self, db, filters):
        """Build a query with filters applied."""
        # Use a proper SQLAlchemy query starting point
        query = db.query(self.sqlalchemy_model)
        
        # Get filter parameters
        filter_dict = self._extract_filter_params(filters)
        
        # Apply each filter with correct attribute reference
        for field_name, value in filter_dict.items():
            if value is not None:
                column = getattr(self.sqlalchemy_model, field_name, None)
                if column is not None:
                    query = query.filter(column == value)
        
        return query

    def _process_record_fields(self, record: Any) -> Dict[str, Any]:
        """Process record fields with proper type handling."""
        result = {}

        for column in self.table.columns:
            value = getattr(record, column.name)
            field_type = get_eq_type(str(column.type))

            # Handle special types
            if isinstance(field_type, JSONBType):
                result[column.name] = process_jsonb_value(value)
            elif isinstance(field_type, ArrayType):
                result[column.name] = process_array_value(value, field_type.item_type)
            else:
                result[column.name] = value

        return result
