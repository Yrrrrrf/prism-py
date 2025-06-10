# src/prism/api/crud.py
import logging
import traceback
from typing import Any, Callable, Dict, List, Optional, Type

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import Table, func, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from prism.api.router import RouteGenerator
from prism.common.types import PrismBaseModel

# Get a logger for this module
log = logging.getLogger(__name__)


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
        self.query_model = create_query_params_model(
            self.pydantic_model,
            self.table.columns,
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
            try:
                inspected_mapper = inspect(self.sqlalchemy_model)
                if column_name in inspected_mapper.c:
                    return inspected_mapper.c[column_name]
            except Exception:
                pass  # Attribute not found through inspection either
            log.warning(
                f"Column '%s' not found on model '%s'.",
                column_name,
                self.sqlalchemy_model.__name__,
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
                        query = query.filter(column_attr == value)
                    except SQLAlchemyError as sa_exc:
                        log.error(
                            "SQLAlchemyError applying filter '%s == %s': %s",
                            field_name,
                            value,
                            sa_exc,
                            exc_info=True,
                        )
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid filter value for '{field_name}'.",
                        )
                    except Exception as e:
                        log.error(
                            "Unexpected error applying filter '%s == %s': %s",
                            field_name,
                            value,
                            e,
                            exc_info=True,
                        )
                        raise HTTPException(
                            status_code=500, detail="Error processing filter."
                        )
                else:
                    raise HTTPException(
                        status_code=400, detail=f"Invalid filter field: '{field_name}'."
                    )
        return query

    def create(self):
        """Generate CREATE route."""

        @self.router.post(
            self.get_route_path(),
            response_model=self.pydantic_model,
            summary=f"Create {self.resource_name}",
            description=f"Create a new {self.resource_name} record",
            tags=[self.schema.upper()],
        )
        def create_resource(
            resource_data: self.pydantic_model,
            db: Session = Depends(self.db_dependency),
        ) -> self.pydantic_model:
            try:
                db_resource = self.sqlalchemy_model(
                    **resource_data.model_dump(exclude_unset=False)
                )
                db.add(db_resource)
                db.commit()
                db.refresh(db_resource)
                return self.pydantic_model.model_validate(db_resource)
            except SQLAlchemyError as sa_exc:
                db.rollback()
                log.error(
                    "SQLAlchemyError during CREATE for %s: %s",
                    self.resource_name,
                    sa_exc,
                    exc_info=True,
                )
                if "violates unique constraint" in str(sa_exc).lower():
                    raise HTTPException(
                        status_code=409,
                        detail=f"Resource creation failed: Duplicate value. Details: {sa_exc.orig}",
                    )
                raise HTTPException(
                    status_code=400,
                    detail=f"Database error during creation: {sa_exc.orig}",
                )
            except Exception as e:
                db.rollback()
                log.error(
                    "Error during CREATE for %s: %s",
                    self.resource_name,
                    e,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Creation failed due to an internal error: {e}",
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
            filters: self.query_model = Depends(),
        ) -> List[self.pydantic_model]:
            try:
                query = db.query(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)

                if self.enhanced_filtering:
                    if hasattr(filters, "order_by") and filters.order_by is not None:
                        order_column_attr = self._get_column_attr(filters.order_by)
                        if order_column_attr is not None:
                            order_dir = getattr(filters, "order_dir", "asc")
                            if order_dir == "desc":
                                query = query.order_by(order_column_attr.desc())
                            else:
                                query = query.order_by(order_column_attr.asc())
                        else:
                            log.warning(
                                "Invalid order_by column '%s' for resource '%s'.",
                                filters.order_by,
                                self.resource_name,
                            )
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
                    ):
                        log.warning(
                            "Invalid limit value '%s' provided. Ignoring limit.",
                            filters.limit,
                        )

                resources_db = query.all()
                return [self.pydantic_model.model_validate(res) for res in resources_db]
            except HTTPException:
                raise
            except SQLAlchemyError as sa_exc:
                log.error(
                    "SQLAlchemyError during READ for %s: %s",
                    self.resource_name,
                    sa_exc,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Database query error: {sa_exc.orig}",
                )
            except Exception as e:
                log.error(
                    "Error during READ for %s: %s\n%s",
                    self.resource_name,
                    e,
                    traceback.format_exc(),
                )
                raise HTTPException(
                    status_code=500, detail=f"Could not retrieve resources: {e}"
                )

    def update(self):
        """Generate UPDATE route."""

        @self.router.put(
            self.get_route_path(),
            response_model=self.pydantic_model,
            summary=f"Update {self.resource_name}",
            description=f"Update {self.resource_name} records. Query parameters identify record(s) to update.",
            tags=[self.schema.upper()],
        )
        def update_resource(
            resource_update_data: self.pydantic_model,
            db: Session = Depends(self.db_dependency),
            filters: self.query_model = Depends(),
        ) -> self.pydantic_model:
            try:
                query = db.query(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)
                db_resources = query.all()

                if not db_resources:
                    raise HTTPException(
                        status_code=404,
                        detail=f"{self.resource_name} not found with specified criteria.",
                    )
                if len(db_resources) > 1:
                    log.warning(
                        "Update criteria matched %d records for %s. Updating all matched.",
                        len(db_resources),
                        self.resource_name,
                    )

                update_data_dict = resource_update_data.model_dump(exclude_none=True)
                for db_resource in db_resources:
                    for key, value in update_data_dict.items():
                        if hasattr(db_resource, key):
                            setattr(db_resource, key, value)
                        else:
                            log.warning(
                                "Field '%s' in update payload not found on model '%s'.",
                                key,
                                self.resource_name,
                            )

                db.commit()
                # For simplicity, always refresh and return the first updated record.
                db.refresh(db_resources[0])
                return self.pydantic_model.model_validate(db_resources[0])

            except HTTPException:
                db.rollback()
                raise
            except SQLAlchemyError as sa_exc:
                db.rollback()
                log.error(
                    "SQLAlchemyError during UPDATE for %s: %s",
                    self.resource_name,
                    sa_exc,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Database error during update: {sa_exc.orig}",
                )
            except Exception as e:
                db.rollback()
                log.error(
                    "Error during UPDATE for %s: %s",
                    self.resource_name,
                    e,
                    exc_info=True,
                )
                raise HTTPException(status_code=500, detail=f"Update failed: {e}")

    def delete(self):
        """Generate DELETE route."""

        @self.router.delete(
            self.get_route_path(),
            response_model=Dict[str, Any],
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
                deleted_count = query.delete(synchronize_session=False)
                db.commit()

                if deleted_count == 0:
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
                    "SQLAlchemyError during DELETE for %s: %s",
                    self.resource_name,
                    sa_exc,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Database error during deletion: {sa_exc.orig}",
                )
            except Exception as e:
                db.rollback()
                log.error(
                    "Error during DELETE for %s: %s",
                    self.resource_name,
                    e,
                    exc_info=True,
                )
                raise HTTPException(status_code=500, detail=f"Deletion failed: {e}")

    def count(self):
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
                pk_column_names = [
                    col.name for col in inspect(self.sqlalchemy_model).primary_key
                ]
                count_expression = (
                    func.count(self._get_column_attr(pk_column_names[0]))
                    if pk_column_names
                    else func.count()
                )

                query = db.query(count_expression).select_from(self.sqlalchemy_model)
                query = self._apply_filters(query, filters)
                total_count = query.scalar_one_or_none()
                return {"count": total_count or 0}

            except HTTPException:
                raise
            except SQLAlchemyError as sa_exc:
                log.error(
                    "SQLAlchemyError during COUNT for %s: %s",
                    self.resource_name,
                    sa_exc,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Database error during count: {sa_exc.orig}",
                )
            except Exception as e:
                log.error(
                    "Error during COUNT for %s: %s",
                    self.resource_name,
                    e,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=500, detail=f"Could not count resources: {e}"
                )
