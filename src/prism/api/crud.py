# src/prism/api/crud.py
from typing import Any, Callable, Dict, List, Optional, Type

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field  # Keep create_model if still used by query_model
from sqlalchemy import Table, func, inspect  # Import func and inspect
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError  # For more specific DB error handling

from prism.api.router import RouteGenerator  # Assuming this is your base
from prism.common.types import (
    PrismBaseModel,
    get_eq_type,
)  # Assuming these are relevant
from prism.core.logging import log  # Your logger


class CrudGenerator(RouteGenerator):
    """Generator for CRUD routes with enhanced error handling and count endpoint."""

    def __init__(
        self,
        table: Table,
        pydantic_model: Type[BaseModel],
        sqlalchemy_model: Type[Any],  # This is the mapped SQLAlchemy class
        router: APIRouter,
        db_dependency: Callable,
        schema: str,
        prefix: str = "",
        enhanced_filtering: bool = True,
    ):
        super().__init__(
            resource_name=table.name,
            router=router,
            db_dependency=db_dependency,
            schema=schema,
            response_model=pydantic_model,  # This is the Pydantic model for responses
            query_model=None,
            table=table,  # This is the SQLAlchemy Table object
            prefix=prefix,
        )
        self.sqlalchemy_model = sqlalchemy_model  # Mapped class for querying
        self.pydantic_model = (
            pydantic_model  # Pydantic model for request/response bodies
        )
        self.enhanced_filtering = enhanced_filtering
        self.initialize()

    def initialize(self):
        """Initialize the generator with query model based on filtering options."""
        from prism.common.types import create_query_params_model  # Moved import here

        # Create query model for filtering, pagination, and sorting
        # This model is used for GET, PUT, DELETE query parameters
        self.query_model = create_query_params_model(
            self.pydantic_model,
            self.table.columns,  # Use table.columns for field names
        )

    def generate_routes(self):
        """Generate all CRUD routes including count."""
        self.create()
        self.read()
        self.update()
        self.delete()
        # todo: Activate when needed...
        # self.count()

    def _get_column_attr(self, column_name: str) -> Optional[Any]:
        """Safely get a column attribute from the SQLAlchemy model."""
        attr = getattr(self.sqlalchemy_model, column_name, None)
        if attr is None:
            # For dynamically mapped classes, attributes might be in _sa_class_manager.mapper.columns
            # or directly in the inspection API
            try:
                # Check SQLAlchemy's inspection API for the column
                inspected_mapper = inspect(self.sqlalchemy_model)
                if column_name in inspected_mapper.c:
                    return inspected_mapper.c[column_name]
            except Exception:  # Be cautious with broad exceptions
                pass  # Attribute not found through inspection either
            log.warn(
                f"Column '{column_name}' not found on model '{self.sqlalchemy_model.__name__}'."
            )
        return attr

    def _apply_filters(self, query: Any, filters_model_instance: BaseModel) -> Any:
        """Apply filters to the SQLAlchemy query."""
        filter_dict = self.extract_filter_params(filters_model_instance)

        for field_name, value in filter_dict.items():
            if value is not None:
                column_attr = self._get_column_attr(field_name)
                if column_attr is not None:
                    try:
                        # Handle specific types like ENUMs if necessary, though SQLAlchemy often handles this.
                        # For example, if your Pydantic model has the enum string, and DB column is Enum type.
                        query = query.filter(column_attr == value)
                    except SQLAlchemyError as sa_exc:
                        log.error(
                            f"SQLAlchemyError applying filter '{field_name} == {value}': {sa_exc}"
                        )
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid filter value for '{field_name}'.",
                        )
                    except Exception as e:
                        log.error(
                            f"Unexpected error applying filter '{field_name} == {value}': {e}"
                        )
                        raise HTTPException(
                            status_code=500, detail="Error processing filter."
                        )
                else:
                    # If a filter is provided for a non-existent column, it's a client error.
                    # Alternatively, log a warning and ignore:
                    # log.warn(f"Filter column '{field_name}' not found on model '{self.resource_name}', ignoring.")
                    raise HTTPException(
                        status_code=400, detail=f"Invalid filter field: '{field_name}'."
                    )
        return query

    def create(self):
        """Generate CREATE route."""

        @self.router.post(
            self.get_route_path(),
            response_model=self.pydantic_model,  # Use the specific Pydantic model
            summary=f"Create {self.resource_name}",
            description=f"Create a new {self.resource_name} record",
            tags=[self.schema.upper()],  # Ensure tags are set
        )
        def create_resource(
            resource_data: self.pydantic_model,  # Body expects the Pydantic model
            db: Session = Depends(self.db_dependency),
        ) -> self.pydantic_model:
            try:
                # Exclude unset values to allow DB defaults, but ensure required Pydantic fields are present
                db_resource = self.sqlalchemy_model(
                    **resource_data.model_dump(exclude_unset=False)
                )
                db.add(db_resource)
                db.commit()
                db.refresh(db_resource)
                # Process for response model (e.g., enum to string, datetime to isoformat)
                # Pydantic should handle this conversion with from_attributes=True
                return self.pydantic_model.model_validate(db_resource)
            except SQLAlchemyError as sa_exc:  # Catch DB-specific errors
                db.rollback()
                log.error(
                    f"SQLAlchemyError during CREATE for {self.resource_name}: {sa_exc}"
                )
                # Provide a more specific error if possible (e.g., integrity violation)
                if "violates unique constraint" in str(sa_exc).lower():
                    raise HTTPException(
                        status_code=409,
                        detail=f"Resource creation failed: Duplicate value. Details: {str(sa_exc.orig if sa_exc.orig else sa_exc)}",
                    )
                raise HTTPException(
                    status_code=400,
                    detail=f"Database error during creation: {str(sa_exc.orig if sa_exc.orig else sa_exc)}",
                )
            except Exception as e:
                db.rollback()
                log.error(f"Error during CREATE for {self.resource_name}: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Creation failed due to an internal error: {str(e)}",
                )

    def read(self):
        """Generate READ route with filtering, pagination, and sorting."""

        @self.router.get(
            self.get_route_path(),
            response_model=List[self.pydantic_model],
            summary=f"Get {self.resource_name} resources",
            description=f"Retrieve {self.resource_name} records with optional filtering, sorting, and pagination",
            tags=[self.schema.upper()],
        )
        def read_resources(
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),  # Pydantic model for query params
        ) -> List[self.pydantic_model]:
            try:
                query = db.query(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)

                if self.enhanced_filtering:
                    if hasattr(filters, "order_by") and filters.order_by is not None:
                        order_column_attr = self._get_column_attr(filters.order_by)
                        if order_column_attr is not None:
                            if (
                                hasattr(filters, "order_dir")
                                and filters.order_dir == "desc"
                            ):
                                query = query.order_by(order_column_attr.desc())
                            else:
                                query = query.order_by(order_column_attr.asc())
                        else:
                            # Log warning or raise 400 if invalid order_by field
                            log.warn(
                                f"Invalid order_by column '{filters.order_by}' for resource '{self.resource_name}'."
                            )
                            # raise HTTPException(status_code=400, detail=f"Invalid order_by field: {filters.order_by}")

                    if (
                        hasattr(filters, "offset")
                        and filters.offset is not None
                        and filters.offset >= 0
                    ):
                        query = query.offset(filters.offset)

                    if (
                        hasattr(filters, "limit")
                        and filters.limit is not None
                        and filters.limit > 0
                    ):
                        query = query.limit(filters.limit)
                    elif (
                        hasattr(filters, "limit")
                        and filters.limit is not None
                        and filters.limit <= 0
                    ):  # handle invalid limit
                        log.warn(
                            f"Invalid limit value '{filters.limit}' provided. Using default or no limit."
                        )

                resources_db = query.all()
                # Validate each resource against the Pydantic model for response
                return [self.pydantic_model.model_validate(res) for res in resources_db]
            except HTTPException:  # Re-raise HTTPExceptions from _apply_filters
                raise
            except SQLAlchemyError as sa_exc:
                log.error(
                    f"SQLAlchemyError during READ for {self.resource_name}: {sa_exc}"
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Database query error: {str(sa_exc.orig if sa_exc.orig else sa_exc)}",
                )
            except Exception as e:
                log.error(f"Error during READ for {self.resource_name}: {e}")
                # Log the full traceback for server-side debugging
                import traceback

                log.error(traceback.format_exc())
                raise HTTPException(
                    status_code=500, detail=f"Could not retrieve resources: {str(e)}"
                )

    def update(self):
        """Generate UPDATE route."""

        @self.router.put(
            self.get_route_path(),
            response_model=self.pydantic_model,  # Typically returns the updated resource
            summary=f"Update {self.resource_name}",
            description=f"Update {self.resource_name} records. Query parameters identify record(s) to update.",
            tags=[self.schema.upper()],
        )
        def update_resource(
            resource_update_data: self.pydantic_model,  # Full model for body, PUT implies replacing the resource
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),  # To identify the record(s) to update
        ) -> self.pydantic_model:
            try:
                # Identify the record(s) to update using filters.
                # For typical PUT /resource/{id}, filters would contain the ID.
                # For PUT /resource?field=value, it might update multiple.
                # This example assumes filters target a single resource for PUT, common pattern.

                query = db.query(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)

                # Fetch the existing resource(s)
                db_resources = query.all()

                if not db_resources:
                    raise HTTPException(
                        status_code=404,
                        detail=f"{self.resource_name} not found with specified criteria.",
                    )

                if len(db_resources) > 1:
                    # This indicates ambiguity if PUT is meant for a single resource.
                    # Consider if your API design allows bulk PUTs via query params.
                    # If PUT is by ID, this shouldn't happen if ID is unique.
                    log.warn(
                        f"Update criteria matched {len(db_resources)} records for {self.resource_name}. Updating all matched."
                    )
                    # raise HTTPException(status_code=400, detail="Update criteria matched multiple resources. Please be more specific.")

                updated_count = 0
                updated_resource_response = None

                # Pydantic model_dump(exclude_unset=True) is good for PATCH.
                # For PUT, usually all fields are provided. model_dump() without exclude_unset is fine.
                # If Pydantic model fields are optional, they might be None.
                update_data_dict = resource_update_data.model_dump(
                    exclude_none=True
                )  # Exclude fields explicitly set to None
                # unless you want to set DB columns to NULL

                for db_resource in db_resources:
                    # Update attributes of the fetched ORM instance
                    for key, value in update_data_dict.items():
                        if hasattr(db_resource, key):
                            setattr(db_resource, key, value)
                        else:
                            log.warn(
                                f"Field '{key}' in update payload not found on model '{self.resource_name}'."
                            )
                    updated_count += 1

                if updated_count > 0:
                    db.commit()
                    # For simplicity, if only one record was updated, refresh and return it.
                    # If multiple were updated, this response strategy needs adjustment.
                    if len(db_resources) == 1:
                        db.refresh(db_resources[0])
                        updated_resource_response = self.pydantic_model.model_validate(
                            db_resources[0]
                        )
                    else:
                        # For multiple updates, returning just the first updated one or a success message
                        db.refresh(
                            db_resources[0]
                        )  # Refresh at least one for potential return
                        updated_resource_response = self.pydantic_model.model_validate(
                            db_resources[0]
                        )
                        log.info(
                            f"Updated {updated_count} records. Returning the first one as sample."
                        )

                if (
                    updated_resource_response is None
                ):  # Should not happen if logic above is correct and update occurred
                    raise HTTPException(
                        status_code=500,
                        detail="Update applied but failed to retrieve updated record.",
                    )

                return updated_resource_response

            except HTTPException:
                db.rollback()
                raise
            except SQLAlchemyError as sa_exc:
                db.rollback()
                log.error(
                    f"SQLAlchemyError during UPDATE for {self.resource_name}: {sa_exc}"
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Database error during update: {str(sa_exc.orig if sa_exc.orig else sa_exc)}",
                )
            except Exception as e:
                db.rollback()
                log.error(f"Error during UPDATE for {self.resource_name}: {e}")
                raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

    def delete(self):
        """Generate DELETE route."""

        @self.router.delete(
            self.get_route_path(),
            response_model=Dict[
                str, Any
            ],  # e.g., {"message": "deleted", "deleted_count": 1}
            summary=f"Delete {self.resource_name}",
            description=f"Delete {self.resource_name} records that match the filter criteria",
            tags=[self.schema.upper()],
        )
        def delete_resource(
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),
        ) -> Dict[str, Any]:
            try:
                query = db.query(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)

                # Perform deletion and get count
                # For .delete(synchronize_session=False), records are not available post-delete in session
                deleted_count = query.delete(synchronize_session=False)
                db.commit()

                if deleted_count == 0:
                    # It's not an error if nothing matched, but good to inform client.
                    # raise HTTPException(status_code=404, detail=f"No {self.resource_name} found matching criteria to delete.")
                    return {
                        "message": f"No {self.resource_name} records found matching criteria.",
                        "deleted_count": 0,
                    }

                return {
                    "message": f"{deleted_count} {self.resource_name} record(s) deleted successfully.",
                    "deleted_count": deleted_count,
                }
            except HTTPException:
                db.rollback()
                raise
            except SQLAlchemyError as sa_exc:
                db.rollback()
                log.error(
                    f"SQLAlchemyError during DELETE for {self.resource_name}: {sa_exc}"
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Database error during deletion: {str(sa_exc.orig if sa_exc.orig else sa_exc)}",
                )
            except Exception as e:
                db.rollback()
                log.error(f"Error during DELETE for {self.resource_name}: {e}")
                raise HTTPException(
                    status_code=500, detail=f"Deletion failed: {str(e)}"
                )

    def count(self):  # New method for /count endpoint
        """Generate COUNT route."""

        @self.router.get(
            self.get_route_path("count"),
            response_model=Dict[str, int],
            summary=f"Count {self.resource_name} resources",
            description=f"Get the total count of {self.resource_name} records, with optional filtering.",
            tags=[self.schema.upper()],
        )
        def count_resources(
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),
        ) -> Dict[str, int]:
            try:
                # Determine a PK column for counting. This is generally more efficient.
                # If no PK, SQLAlchemy's count() on model should work.
                # For func.count(), it's good to specify a column, often a PK.

                pk_column_names = [
                    col.name for col in inspect(self.sqlalchemy_model).primary_key
                ]
                if not pk_column_names:
                    # Fallback if no PK defined on the model (unlikely for tables)
                    # This would count all rows.
                    count_expression = func.count()
                else:
                    # Count based on the first primary key column
                    count_expression = func.count(
                        self._get_column_attr(pk_column_names[0])
                    )

                # Start with the count query on the specific expression
                query = db.query(count_expression).select_from(self.sqlalchemy_model)

                # Apply filters
                query = self._apply_filters(query, filters)

                total_count = (
                    query.scalar_one_or_none()
                )  # scalar_one_or_none is good for aggregate results

                return {"count": total_count if total_count is not None else 0}
            except HTTPException:
                raise
            except SQLAlchemyError as sa_exc:
                log.error(
                    f"SQLAlchemyError during COUNT for {self.resource_name}: {sa_exc}"
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Database error during count: {str(sa_exc.orig if sa_exc.orig else sa_exc)}",
                )
            except Exception as e:
                log.error(f"Error during COUNT for {self.resource_name}: {e}")
                raise HTTPException(
                    status_code=500, detail=f"Could not count resources: {str(e)}"
                )
