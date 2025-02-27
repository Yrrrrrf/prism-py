# src/forge/api/crud.py
from typing import Any, Callable, Dict, List, Optional, Type, Union
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import Table
from pydantic import BaseModel, create_model, Field

from forge.api.router import RouteGenerator
from forge.common.types import ForgeBaseModel, get_eq_type, JSONBType, ArrayType


class CrudGenerator(RouteGenerator):
    """Generator for CRUD routes."""

    def __init__(
        self,
        table: Table,
        pydantic_model: Type[BaseModel],
        sqlalchemy_model: Type[Any],
        router: APIRouter,
        db_dependency: Callable,
        schema: str,
        prefix: str = "",
        enhanced_filtering: bool = True,  # Enable/disable advanced filtering
    ):
        super().__init__(
            resource_name=table.name,
            router=router,
            db_dependency=db_dependency,
            schema=schema,
            response_model=pydantic_model,
            query_model=None,  # Will be created in initialize
            table=table,
            prefix=prefix,
        )
        self.sqlalchemy_model = sqlalchemy_model
        self.enhanced_filtering = enhanced_filtering
        self.initialize()

    def initialize(self):
        """Initialize the generator with query model based on filtering options."""
        from forge.common.types import create_query_params_model, QueryParams

        if self.enhanced_filtering:
            # Create query model with enhanced filtering options
            self.query_model = create_query_params_model(
                self.response_model, self.table.columns
            )
        else:
            # Create simpler query model with only table fields as filters
            fields = {}
            for column in self.table.columns:
                field_type = get_eq_type(str(column.type))
                # Make all fields optional for query params
                fields[column.name] = (Optional[field_type], Field(default=None))

            # Create the model without additional filtering params
            self.query_model = create_model(
                f"{self.response_model.__name__}QueryParams",
                **fields,
                __base__=ForgeBaseModel,
            )

    def generate_routes(self):
        """Generate all CRUD routes."""
        self.create()
        self.read()
        self.update()
        self.delete()

    def create(self):
        """Generate CREATE route."""

        @self.router.post(
            self.get_route_path(),
            response_model=self.response_model,
            summary=f"Create {self.resource_name}",
            description=f"Create a new {self.resource_name} record",
        )
        def create_resource(
            resource: self.response_model, db: Session = Depends(self.db_dependency)
        ) -> self.response_model:
            # Extract data excluding unset values
            data = resource.model_dump(exclude_unset=True)

            try:
                # Create new record instance
                db_resource = self.sqlalchemy_model(**data)
                db.add(db_resource)
                db.commit()
                db.refresh(db_resource)

                # Process and return result
                result_dict = self.process_record_fields(db_resource)
                return self.response_model(**result_dict)
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=400, detail=f"Creation failed: {str(e)}"
                )

    def read(self):
        """Generate READ route with filtering and pagination."""

        @self.router.get(
            self.get_route_path(),
            response_model=List[self.response_model],
            summary=f"Get {self.resource_name} resources",
            description=f"Retrieve {self.resource_name} records with optional filtering",
        )
        def read_resources(
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),
        ) -> List[self.response_model]:
            # Start with base query
            query = db.query(self.sqlalchemy_model)

            # Apply filters
            query = self._apply_filters(query, filters)

            # Apply pagination and sorting if enhanced filtering is enabled
            if self.enhanced_filtering:
                if hasattr(filters, "limit") and filters.limit is not None:
                    query = query.limit(filters.limit)

                if hasattr(filters, "offset") and filters.offset is not None:
                    query = query.offset(filters.offset)

                # Apply ordering
                if hasattr(filters, "order_by") and filters.order_by is not None:
                    order_column = getattr(
                        self.sqlalchemy_model, filters.order_by, None
                    )
                    if order_column:
                        if (
                            hasattr(filters, "order_dir")
                            and filters.order_dir == "desc"
                        ):
                            query = query.order_by(order_column.desc())
                        else:
                            query = query.order_by(order_column)

            # Execute query
            resources = query.all()

            # Process and validate results
            processed_results = []
            for resource in resources:
                processed_record = self.process_record_fields(resource)
                try:
                    validated_record = self.response_model.model_validate(
                        processed_record
                    )
                    processed_results.append(validated_record)
                except Exception as e:
                    # Log validation error but continue processing other records
                    pass

            return processed_results

    def update(self):
        """Generate UPDATE route."""

        @self.router.put(
            self.get_route_path(),
            response_model=Dict[str, Any],
            summary=f"Update {self.resource_name}",
            description=f"Update {self.resource_name} records that match the filter criteria",
        )
        def update_resource(
            resource: self.response_model,
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),
        ) -> Dict[str, Any]:
            # Get update data
            update_data = resource.model_dump(exclude_unset=True)

            # Check for filter params
            filter_dict = self.extract_filter_params(filters)
            if not filter_dict:
                raise HTTPException(status_code=400, detail="No filters provided")

            try:
                # Build query with filters
                query = db.query(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)

                # Get records before update
                resources_before = query.all()
                if not resources_before:
                    raise HTTPException(
                        status_code=404, detail="No matching resources found"
                    )

                # Store old data for response
                old_data = [
                    self.response_model.model_validate(
                        self.process_record_fields(resource)
                    )
                    for resource in resources_before
                ]

                # Perform update
                updated_count = query.update(update_data)
                db.commit()

                # Get updated records
                query = db.query(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)
                resources_after = query.all()

                # Process updated data
                updated_data = [
                    self.response_model.model_validate(
                        self.process_record_fields(resource)
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

    def delete(self):
        """Generate DELETE route."""

        @self.router.delete(
            self.get_route_path(),
            response_model=Dict[str, Any],
            summary=f"Delete {self.resource_name}",
            description=f"Delete {self.resource_name} records that match the filter criteria",
        )
        def delete_resource(
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),
        ) -> Dict[str, Any]:
            # Check for filter params
            filter_dict = self.extract_filter_params(filters)
            if not filter_dict:
                raise HTTPException(status_code=400, detail="No filters provided")

            try:
                # Build query with filters
                query = db.query(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)

                # Get resources before deletion
                to_delete = query.all()
                if not to_delete:
                    return {"message": "No resources found matching the criteria"}

                # Store deleted data for response
                deleted_resources = [
                    self.response_model.model_validate(
                        self.process_record_fields(resource)
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

    def _apply_filters(self, query, filters):
        """Apply filters to the query."""
        # Extract filter parameters
        filter_dict = self.extract_filter_params(filters)

        # Apply each filter
        for field_name, value in filter_dict.items():
            if value is not None:
                column = getattr(self.sqlalchemy_model, field_name, None)
                if column is not None:
                    query = query.filter(column == value)

        return query
