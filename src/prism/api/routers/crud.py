# src/prism/api/routers/crud.py

from typing import Any, Callable, Dict, List, Optional, Type, Union
from inspect import Parameter, signature

import sqlalchemy
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, create_model
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from prism.api.routers import gen_openapi_parameters
from prism.core.models.tables import TableMetadata
from prism.core.query.builder import QueryBuilder
from prism.core.query.operators import SQL_OPERATOR_MAP
from prism.core.types.utils import ArrayType, JSONBType, get_python_type
from prism.ui import console, display_table_structure

# --- Helper Functions ---


def get_query_params(request: Request) -> Dict[str, Any]:
    """Dependency to capture all query parameters from a request."""
    return dict(request.query_params)


# --- Main Generator Class ---


class CrudGenerator:
    """
    Generates full CRUD+PATCH API routes for a given table.
    Adaptively handles tables with single or composite (multi-column) primary keys.
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
        self.is_multi_pk = len(self.table_meta.primary_key_columns) > 1

        # --- PERFORMANCE OPTIMIZATION ---
        # For multi-PK tables, reflect the table structure ONCE at startup
        # and cache the object to avoid expensive per-request reflection.
        self.table_obj = None
        if self.is_multi_pk:
            self.table_obj = sqlalchemy.Table(
                self.table_meta.name,
                sqlalchemy.MetaData(),
                schema=self.table_meta.schema,
                autoload_with=self.engine,
            )
        # --------------------------------

        self.sqlalchemy_model = self._get_sqlalchemy_model()

        # Pydantic models are generated for all tables for validation and serialization.
        self.pydantic_create_model = self._create_pydantic_input_model(is_update=False)
        self.pydantic_partial_update_model = self._create_pydantic_input_model(
            is_update=True
        )
        self.pydantic_read_model = self._create_pydantic_read_model()

    def generate_routes(self):
        """Main dispatcher to generate routes based on the table's key structure."""
        if not self.pydantic_read_model:
            return

        display_table_structure(self.table_meta)

        if self.is_multi_pk:
            console.print(
                f"  🔵 Table [bold]{self.table_meta.schema}.{self.table_meta.name}[/] has a composite primary key. Using SQLAlchemy Core routes."
            )
            self._add_multi_pk_routes()
        elif self.sqlalchemy_model:
            console.print(
                f"  🟢 Table [bold]{self.table_meta.schema}.{self.table_meta.name}[/] has a single primary key. Using SQLAlchemy ORM routes."
            )
            self._add_single_pk_routes()
        else:
            console.print(
                f"  🟡 Skipping route generation for table {self.table_meta.schema}.{self.table_meta.name}: Automap failed and it's not a multi-PK table."
            )

    # --- Route Generation Dispatchers ---

    def _add_single_pk_routes(self):
        """Generates all routes for a standard, single-PK table using the ORM."""
        self._add_read_list_route()
        self._add_create_route()
        self._add_update_route()
        self._add_patch_route()
        self._add_delete_route()

    def _add_multi_pk_routes(self):
        """Generates all routes for a multi-PK table using SQLAlchemy Core."""
        self._add_multi_pk_read_route()
        self._add_multi_pk_create_route()
        self._add_multi_pk_update_route()
        self._add_multi_pk_delete_route()

    # --- Multi-PK Route Generation (SQLAlchemy Core) ---

    def _add_multi_pk_read_route(self):
        """
        Generates a single, smart GET endpoint for multi-PK tables that handles
        both fetching a single record by its composite key and listing/filtering records.
        """

        def read_multi_pk_resources(
            request: Request, db: Session = Depends(self.db_dependency)
        ) -> Union[List[Dict], Dict]:
            query_params = dict(request.query_params)
            pk_names = set(self.table_meta.primary_key_columns)
            provided_keys = set(query_params.keys())
            table_obj = self.table_obj  # Use the cached table object

            # --- LOGIC BRANCH 1: Fetch a SINGLE record by composite PK ---
            if pk_names.issubset(provided_keys):
                pk_filters = {k: query_params[k] for k in pk_names}
                stmt = sqlalchemy.select(table_obj).where(
                    sqlalchemy.and_(
                        *[table_obj.c[k] == v for k, v in pk_filters.items()]
                    )
                )
                result = db.execute(stmt).first()
                if not result:
                    raise HTTPException(status_code=404, detail="Record not found.")
                return result._asdict()

            # --- LOGIC BRANCH 2: LIST and FILTER records ---
            else:

                class TempModel:
                    pass

                for col in self.table_meta.columns:
                    setattr(TempModel, col.name, None)

                processed_params = {
                    f"{k}[eq]" if hasattr(TempModel, k) else k: v
                    for k, v in query_params.items()
                }
                base_query = (
                    f"SELECT * FROM {self.table_meta.schema}.{self.table_meta.name}"
                )
                builder = QueryBuilder(model=TempModel, params=processed_params)
                where, order, limit, offset, params = builder.build_clauses()

                final_query = f"{base_query} {where} {order} {limit} {offset}"
                result = db.execute(sqlalchemy.text(final_query), params)
                return result.mappings().all()

        response_model = Union[List[self.pydantic_read_model], self.pydantic_read_model]
        description = self._generate_multi_pk_read_description()

        self.router.add_api_route(
            path=f"/{self.table_meta.name}",
            endpoint=read_multi_pk_resources,
            methods=["GET"],
            response_model=response_model,
            summary=f"Get or filter {self.table_meta.name} records",
            description=description,
            openapi_extra={"parameters": gen_openapi_parameters(self.table_meta)},
        )

    def _add_multi_pk_create_route(self):
        create_model = self._create_pydantic_input_model(is_multi_pk=True)

        def create_multi_pk_resource(
            resource_data: create_model,
            db: Session = Depends(self.db_dependency),
        ):
            table_obj = self.table_obj
            stmt = sqlalchemy.insert(table_obj).values(**resource_data.model_dump())
            try:
                db.execute(stmt)
                pk_filters = {
                    k: getattr(resource_data, k)
                    for k in self.table_meta.primary_key_columns
                }
                fetch_stmt = sqlalchemy.select(table_obj).where(
                    sqlalchemy.and_(
                        *[table_obj.c[k] == v for k, v in pk_filters.items()]
                    )
                )
                new_record = db.execute(fetch_stmt).first()
                db.commit()
                return new_record
            except sqlalchemy.exc.IntegrityError as e:
                db.rollback()
                raise HTTPException(
                    status_code=409,
                    detail=f"Conflict: Record likely already exists. Details: {e.orig}",
                )
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=400, detail=f"Failed to create record: {e}"
                )

        self.router.post(
            f"/{self.table_meta.name}",
            response_model=self.pydantic_read_model,
            status_code=201,
            summary=f"Create a new {self.table_meta.name} record with a composite key",
        )(create_multi_pk_resource)

    def _add_multi_pk_update_route(self):
        def update_multi_pk_resource(
            request: Request,
            resource_data: self.pydantic_partial_update_model,
            db: Session = Depends(self.db_dependency),
        ):
            pk_filters = self._get_pk_filters_from_query_params(
                dict(request.query_params)
            )
            update_values = resource_data.model_dump(exclude_unset=True)
            if not update_values:
                raise HTTPException(
                    status_code=400, detail="No fields to update provided."
                )

            table_obj = self.table_obj
            stmt = (
                sqlalchemy.update(table_obj)
                .where(
                    sqlalchemy.and_(
                        *[table_obj.c[k] == v for k, v in pk_filters.items()]
                    )
                )
                .values(**update_values)
            )

            result = db.execute(stmt)
            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail="Record not found.")

            fetch_stmt = sqlalchemy.select(table_obj).where(
                sqlalchemy.and_(*[table_obj.c[k] == v for k, v in pk_filters.items()])
            )
            updated_record = db.execute(fetch_stmt).first()
            db.commit()
            return updated_record

        self.router.put(
            f"/{self.table_meta.name}",
            response_model=self.pydantic_read_model,
            summary=f"Update a {self.table_meta.name} record by composite primary key",
        )(update_multi_pk_resource)

    def _add_multi_pk_delete_route(self):
        def delete_multi_pk_resource(
            request: Request, db: Session = Depends(self.db_dependency)
        ):
            pk_filters = self._get_pk_filters_from_query_params(
                dict(request.query_params)
            )
            table_obj = self.table_obj
            stmt = sqlalchemy.delete(table_obj).where(
                sqlalchemy.and_(*[table_obj.c[k] == v for k, v in pk_filters.items()])
            )
            result = db.execute(stmt)
            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail="Record not found.")
            db.commit()
            return Response(status_code=204)

        self.router.delete(
            f"/{self.table_meta.name}",
            status_code=204,
            summary=f"Delete a {self.table_meta.name} record by composite primary key",
        )(delete_multi_pk_resource)

    # --- Single-PK Route Generation (SQLAlchemy ORM) ---

    def _add_read_list_route(self):
        def read_resources(
            db: Session = Depends(self.db_dependency),
            query_params: Dict[str, Any] = Depends(get_query_params),
        ) -> List[Any]:
            initial_query = db.query(self.sqlalchemy_model)
            builder = QueryBuilder(self.sqlalchemy_model, query_params)
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

        self.router.post(
            f"/{self.table_meta.name}",
            response_model=self.pydantic_read_model,
            status_code=201,
            summary=f"Create a new {self.table_meta.name} record",
        )(create_resource)

    def _add_update_route(self):
        pk_col_name, pk_type = self._get_single_pk_info()

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

        self.router.put(
            f"/{self.table_meta.name}/{{{pk_col_name}}}",
            response_model=self.pydantic_read_model,
            summary=f"Update a {self.table_meta.name} record by its primary key",
        )(update_resource)

    def _add_patch_route(self):
        pk_col_name, pk_type = self._get_single_pk_info()

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
                    status_code=400, detail="No fields to update provided."
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

        self.router.patch(
            f"/{self.table_meta.name}/{{{pk_col_name}}}",
            response_model=self.pydantic_read_model,
            summary=f"Partially update a {self.table_meta.name} record",
        )(patch_resource)

    def _add_delete_route(self):
        pk_col_name, pk_type = self._get_single_pk_info()

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

        self.router.delete(
            f"/{self.table_meta.name}/{{{pk_col_name}}}",
            status_code=204,
            summary=f"Delete a {self.table_meta.name} record by its primary key",
        )(delete_resource)

    # --- Model Generation & Helpers ---

    def _get_sqlalchemy_model(self) -> Optional[Type]:
        if self.is_multi_pk:
            return None
        Base = automap_base()
        try:
            Base.prepare(self.engine, reflect=True, schema=self.table_meta.schema)
            model_class = getattr(Base.classes, self.table_meta.name, None)
            if model_class is None:
                return None
            return model_class
        except Exception:
            return None

    def _create_pydantic_read_model(self) -> Type[BaseModel]:
        fields = {}
        for col in self.table_meta.columns:
            if col.enum_info:
                pydantic_type = col.enum_info.to_python_enum()
            else:
                internal_type = get_python_type(col.sql_type, col.is_nullable)
                # Convert our wrapper types to what Pydantic understands
                if isinstance(internal_type, JSONBType):
                    pydantic_type = Any
                elif isinstance(internal_type, ArrayType):
                    # Handle nested JSONB in arrays
                    item_type = (
                        Any
                        if isinstance(internal_type.item_type, JSONBType)
                        else internal_type.item_type
                    )
                    pydantic_type = List[item_type]
                else:
                    pydantic_type = internal_type

            # Now, correctly make the pydantic_type optional
            final_type = (
                pydantic_type | None
                if col.is_nullable and not col.enum_info
                else pydantic_type
            )
            fields[col.name] = (final_type, ...)

        return create_model(
            f"{self.table_meta.name.capitalize()}ReadModel",
            **fields,
            __config__=ConfigDict(from_attributes=True, use_enum_values=True),
        )

    def _create_pydantic_input_model(
        self, is_update: bool = False, is_multi_pk: bool = False
    ) -> Type[BaseModel]:
        fields = {}
        for col in self.table_meta.columns:
            if (
                not is_multi_pk
                and not is_update
                and (col.is_pk or col.default_value is not None)
            ):
                continue

            if col.enum_info:
                pydantic_type = col.enum_info.to_python_enum()
            else:
                internal_type = get_python_type(col.sql_type, col.is_nullable)
                if isinstance(internal_type, JSONBType):
                    pydantic_type = Any
                elif isinstance(internal_type, ArrayType):
                    item_type = (
                        Any
                        if isinstance(internal_type.item_type, JSONBType)
                        else internal_type.item_type
                    )
                    pydantic_type = List[item_type]
                else:
                    pydantic_type = internal_type

            field_info = {}
            if (
                col.max_length
                and isinstance(pydantic_type, type)
                and issubclass(pydantic_type, str)
            ):
                field_info["max_length"] = col.max_length

            # For updates, all fields are optional.
            if is_update:
                # Correctly create an optional type
                final_type = Optional[pydantic_type]
                fields[col.name] = (final_type, Field(default=None, **field_info))
            else:
                # Handle non-update case
                final_type = pydantic_type | None if col.is_nullable else pydantic_type
                fields[col.name] = (
                    final_type,
                    Field(default=... if not col.is_nullable else None, **field_info),
                )

        prefix = "PartialUpdate" if is_update else "Create"
        return create_model(
            f"{prefix}{self.table_meta.name.capitalize()}Model", **fields
        )

    def _get_single_pk_info(self) -> tuple[str, Type]:
        pk_col_name = self.table_meta.primary_key_columns[0]
        pk_col = next(c for c in self.table_meta.columns if c.name == pk_col_name)
        pk_type = get_python_type(pk_col.sql_type, nullable=False)
        return pk_col_name, pk_type

    def _get_pk_filters_from_query_params(
        self, query_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        pk_filters = {
            k: query_params.get(k) for k in self.table_meta.primary_key_columns
        }
        if not all(pk_filters.values()):
            raise HTTPException(
                status_code=400,
                detail=f"All primary key fields must be provided as query parameters: {', '.join(self.table_meta.primary_key_columns)}",
            )
        return pk_filters

    def _generate_endpoint_description(self) -> str:
        # Generic description for listing endpoints
        return self._generate_multi_pk_read_description(is_multi_pk=False)

    def _generate_multi_pk_read_description(self, is_multi_pk: bool = True) -> str:
        description_parts = [f"Retrieve records from `{self.table_meta.name}`."]
        if is_multi_pk:
            pk_list = f"`{', '.join(self.table_meta.primary_key_columns)}`"
            description_parts.append(
                f"\n\n**To fetch a single record, you must provide all primary key fields as query parameters:** {pk_list}."
            )
            description_parts.append(
                "\n\n**To list and filter multiple records**, use any other combination of query parameters."
            )

        description_parts.append(
            "\n\n### Advanced Filtering\nFor more complex queries, use the `field[operator]=value` syntax."
        )
        ops = f"`{', '.join(SQL_OPERATOR_MAP.keys())}`"
        description_parts.append(f"\n- **Available Operators:** {ops}")
        description_parts.append(
            "- **Example:** `?age[gte]=18&status[in]=active,pending`"
        )
        return "".join(description_parts)
