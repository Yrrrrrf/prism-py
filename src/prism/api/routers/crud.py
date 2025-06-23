# src/prism/api/routers/crud.py
from typing import Any, Callable, Dict, List, Optional, Type

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, create_model
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from prism.api.routers import gen_openapi_parameters

from prism.core.models.tables import TableMetadata
from prism.core.query.builder import QueryBuilder
from prism.core.query.operators import SQL_OPERATOR_MAP
from prism.core.types.utils import (
    ArrayType,
    JSONBType,
    get_python_type,
)


def get_query_params(request: Request) -> Dict[str, Any]:
    return dict(request.query_params)


class CrudGenerator:
    """
    Generates full CRUD+PATCH API routes for a given table.
    """

    def __init__(
        self,
        table_metadata: TableMetadata,
        db_dependency: Callable[..., Session],
        router: APIRouter,
        engine,
    ):
        self.table_meta = table_metadata
        self.db_dependency = db_dependency
        self.router = router
        self.engine = engine

        self.sqlalchemy_model = self._get_sqlalchemy_model()

        if self.sqlalchemy_model:
            self.pydantic_create_model = self._create_pydantic_input_model(
                is_update=False
            )
            self.pydantic_partial_update_model = self._create_pydantic_input_model(
                is_update=True
            )
            self.pydantic_read_model = self._create_pydantic_read_model()
        else:
            self.pydantic_create_model = None
            self.pydantic_partial_update_model = None
            self.pydantic_read_model = None

    def generate_routes(self):
        if not self.sqlalchemy_model or not self.pydantic_read_model:
            return

        self._add_read_route()
        self._add_create_route()
        self._add_update_route()
        self._add_patch_route()
        self._add_delete_route()

        print(
            f"  ✓ Generated FULL CRUD+PATCH routes for {self.table_meta.schema}.{self.table_meta.name}"
        )

    def _get_sqlalchemy_model(self) -> Optional[Type]:
        Base = automap_base()
        try:
            Base.prepare(self.engine, reflect=True, schema=self.table_meta.schema)
            model_class = getattr(Base.classes, self.table_meta.name, None)
            if model_class is None:
                print(
                    f"  🟡 Skipping table {self.table_meta.schema}.{self.table_meta.name}: Could not automap. (Likely missing a primary key)."
                )
                return None
            return model_class
        except Exception as e:
            print(
                f"  ❌ Skipping table {self.table_meta.schema}.{self.table_meta.name}: An unexpected error occurred during automap: {e}"
            )
            return None

    def _create_pydantic_read_model(self) -> Type[BaseModel]:
        fields = {}
        for col in self.table_meta.columns:
            internal_type = get_python_type(col.sql_type, col.is_nullable)
            pydantic_type: Type = (
                Any
                if isinstance(internal_type, JSONBType)
                else (
                    List[Any]
                    if isinstance(internal_type, ArrayType)
                    and isinstance(internal_type.item_type, JSONBType)
                    else (
                        List[internal_type.item_type]
                        if isinstance(internal_type, ArrayType)
                        else internal_type
                    )
                )
            )
            final_type = pydantic_type | None if col.is_nullable else pydantic_type
            fields[col.name] = (final_type, None if col.is_nullable else ...)
        return create_model(
            f"{self.table_meta.name.capitalize()}ReadModel",
            **fields,
            __config__=ConfigDict(from_attributes=True),
        )

    def _create_pydantic_input_model(self, is_update: bool = False) -> Type[BaseModel]:
        fields = {}
        for col in self.table_meta.columns:
            if col.is_pk or col.default_value is not None:
                continue
            internal_type = get_python_type(col.sql_type, col.is_nullable)
            pydantic_type: Type = (
                Any
                if isinstance(internal_type, JSONBType)
                else (
                    List[Any]
                    if isinstance(internal_type, ArrayType)
                    and isinstance(internal_type.item_type, JSONBType)
                    else (
                        List[internal_type.item_type]
                        if isinstance(internal_type, ArrayType)
                        else internal_type
                    )
                )
            )
            final_type = (
                pydantic_type | None if is_update or col.is_nullable else pydantic_type
            )
            default_value = None if is_update or col.is_nullable else ...
            fields[col.name] = (final_type, default_value)
        model_name_prefix = "PartialUpdate" if is_update else "Create"
        return create_model(
            f"{model_name_prefix}{self.table_meta.name.capitalize()}Model", **fields
        )

    def _generate_endpoint_description(self) -> str:
        fields_list = "\n".join(
            f"- `{col.name}`"
            for col in self.table_meta.columns
            if not isinstance(
                get_python_type(col.sql_type, False), (ArrayType, JSONBType)
            )
        )
        return f"""Retrieve records from `{self.table_meta.name}`.\n\nSimple equality filters can be applied directly via the parameters below.\n\n### Advanced Filtering\nFor more complex queries, use the `field[operator]=value` syntax.\n\n- **Available Operators:** `{", ".join(f"`{op}`" for op in SQL_OPERATOR_MAP.keys())}`\n- **Example:** `?age[gte]=18&status[in]=active,pending`\n\n### Filterable Fields\n{fields_list}"""

    def _add_read_route(self):
        class SimpleQueryBuilder(QueryBuilder):
            def _apply_filters(self):
                processed_params = {
                    f"{k}[eq]" if hasattr(self.model, k) else k: v
                    for k, v in self.params.items()
                }
                self.params = processed_params
                super()._apply_filters()

        def read_resources(
            db: Session = Depends(self.db_dependency),
            query_params: Dict[str, Any] = Depends(get_query_params),
        ) -> List[Any]:
            initial_query = db.query(self.sqlalchemy_model)
            builder = SimpleQueryBuilder(self.sqlalchemy_model, query_params)
            query = builder.build(initial_query)
            return query.all()

        self.router.add_api_route(
            path=f"/{self.table_meta.name}",
            endpoint=read_resources,
            methods=["GET"],
            response_model=List[self.pydantic_read_model],
            summary=f"Read and filter {self.table_meta.name} records",
            description=self._generate_endpoint_description(),
            openapi_extra={"parameters": gen_openapi_parameters(self.table_meta)},
        )

    def _add_create_route(self):
        @self.router.post(
            f"/{self.table_meta.name}",
            response_model=self.pydantic_read_model,
            status_code=201,
            summary=f"Create a new {self.table_meta.name} record",
        )
        def create_resource(
            resource_data: self.pydantic_create_model,
            db: Session = Depends(self.db_dependency),
        ) -> Any:
            try:
                db_resource = self.sqlalchemy_model(**resource_data.model_dump())
                db.add(db_resource)
                db.commit()
                db.refresh(db_resource)
                return db_resource
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=400, detail=f"Failed to create record: {e}"
                )

    def _add_update_route(self):
        if not self.table_meta.primary_key_columns:
            return
        pk_col_name = self.table_meta.primary_key_columns[0]
        pk_col = next(c for c in self.table_meta.columns if c.name == pk_col_name)
        pk_type = get_python_type(pk_col.sql_type, nullable=False)

        @self.router.put(
            f"/{self.table_meta.name}/{{{pk_col_name}}}",
            response_model=self.pydantic_read_model,
            summary=f"Update a {self.table_meta.name} record by its primary key",
        )
        def update_resource(
            pk_value: pk_type,
            resource_data: self.pydantic_partial_update_model,
            db: Session = Depends(self.db_dependency),
        ) -> Any:
            db_resource = (
                db.query(self.sqlalchemy_model)
                .filter(getattr(self.sqlalchemy_model, pk_col_name) == pk_value)
                .first()
            )
            if not db_resource:
                raise HTTPException(
                    status_code=404,
                    detail=f"Record with {pk_col_name}='{pk_value}' not found",
                )
            for key, value in resource_data.model_dump(exclude_unset=True).items():
                setattr(db_resource, key, value)
            try:
                db.commit()
                db.refresh(db_resource)
                return db_resource
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=400, detail=f"Failed to update record: {e}"
                )

    def _add_patch_route(self):
        if not self.table_meta.primary_key_columns:
            return
        pk_col_name = self.table_meta.primary_key_columns[0]
        pk_col = next(c for c in self.table_meta.columns if c.name == pk_col_name)
        pk_type = get_python_type(pk_col.sql_type, nullable=False)

        @self.router.patch(
            f"/{self.table_meta.name}/{{{pk_col_name}}}",
            response_model=self.pydantic_read_model,
            summary=f"Partially update a {self.table_meta.name} record",
        )
        def patch_resource(
            pk_value: pk_type,
            resource_data: self.pydantic_partial_update_model,
            db: Session = Depends(self.db_dependency),
        ) -> Any:
            db_resource = (
                db.query(self.sqlalchemy_model)
                .filter(getattr(self.sqlalchemy_model, pk_col_name) == pk_value)
                .first()
            )
            if not db_resource:
                raise HTTPException(
                    status_code=404,
                    detail=f"Record with {pk_col_name}='{pk_value}' not found",
                )
            update_data = resource_data.model_dump(exclude_unset=True)
            if not update_data:
                raise HTTPException(
                    status_code=400,
                    detail="No fields to update provided in the request body.",
                )
            for key, value in update_data.items():
                setattr(db_resource, key, value)
            try:
                db.commit()
                db.refresh(db_resource)
                return db_resource
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=400, detail=f"Failed to update record: {e}"
                )

    def _add_delete_route(self):
        if not self.table_meta.primary_key_columns:
            return
        pk_col_name = self.table_meta.primary_key_columns[0]
        pk_col = next(c for c in self.table_meta.columns if c.name == pk_col_name)
        pk_type = get_python_type(pk_col.sql_type, nullable=False)

        @self.router.delete(
            f"/{self.table_meta.name}/{{{pk_col_name}}}",
            status_code=204,
            summary=f"Delete a {self.table_meta.name} record by its primary key",
        )
        def delete_resource(
            pk_value: pk_type, db: Session = Depends(self.db_dependency)
        ):
            db_resource = (
                db.query(self.sqlalchemy_model)
                .filter(getattr(self.sqlalchemy_model, pk_col_name) == pk_value)
                .first()
            )
            if not db_resource:
                raise HTTPException(
                    status_code=404,
                    detail=f"Record with {pk_col_name}='{pk_value}' not found",
                )
            try:
                db.delete(db_resource)
                db.commit()
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=400, detail=f"Failed to delete record: {e}"
                )
