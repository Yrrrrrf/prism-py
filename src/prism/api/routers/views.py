# src/prism/api/routers/views.py
from typing import Any, Callable, Dict, List, Type

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, create_model
from sqlalchemy import text
from sqlalchemy.orm import Session

from prism.api.routers import gen_openapi_parameters
from prism.core.models.tables import TableMetadata
from prism.core.query.builder import QueryBuilder
from prism.core.query.operators import SQL_OPERATOR_MAP
from prism.core.types.utils import ArrayType, JSONBType, get_python_type
from prism.ui import console, display_table_structure


def get_query_params(request: Request) -> Dict[str, Any]:
    return dict(request.query_params)


class ViewGenerator:
    """Generates read-only, filterable API routes for a database view."""

    def __init__(
        self,
        view_metadata: TableMetadata,
        db_dependency: Callable[..., Session],
        router: APIRouter,
    ):
        self.view_meta = view_metadata
        self.db_dependency = db_dependency
        self.router = router
        self.pydantic_read_model = self._create_pydantic_read_model()

    def generate_routes(self):
        """Generates the read-only route and logs the view's structure."""
        display_table_structure(self.view_meta)
        self._add_read_route()

    def _create_pydantic_read_model(self) -> Type[BaseModel]:
        fields = {}
        for col in self.view_meta.columns:
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
            f"{self.view_meta.name.capitalize()}ViewReadModel",
            **fields,
            __config__=ConfigDict(from_attributes=True),
        )

    def _generate_endpoint_description(self) -> str:
        fields_list = "\n".join(
            f"- `{col.name}`"
            for col in self.view_meta.columns
            if not isinstance(
                get_python_type(col.sql_type, False), (ArrayType, JSONBType)
            )
        )
        return f"""Retrieve records from view `{self.view_meta.name}`.\n\nSimple equality filters can be applied directly via the parameters below.\n\n### Advanced Filtering\nFor more complex queries, use the `field[operator]=value` syntax.\n\n- **Available Operators:** `{", ".join(f"`{op}`" for op in SQL_OPERATOR_MAP.keys())}`\n- **Example:** `?age[gte]=18&status[in]=active,pending`\n\n### Filterable Fields\n{fields_list}"""

    def _add_read_route(self):
        def read_resources(
            db: Session = Depends(self.db_dependency),
            query_params: Dict[str, Any] = Depends(get_query_params),
        ) -> List[Any]:
            class TempModel:
                pass

            for col in self.view_meta.columns:
                setattr(TempModel, col.name, None)

            processed_params = {
                f"{k}[eq]" if hasattr(TempModel, k) else k: v
                for k, v in query_params.items()
            }
            base_query = f"SELECT * FROM {self.view_meta.schema}.{self.view_meta.name}"
            where_clause, order_clause, limit_clause, offset_clause, params = (
                QueryBuilder(
                    model=TempModel,
                    params=processed_params,
                ).build_clauses()
            )
            final_query = f"{base_query} {where_clause} {order_clause} {limit_clause} {offset_clause}"
            result = db.execute(text(final_query), params)
            return result.mappings().all()

        read_resources.__name__ = f"read_view_{self.view_meta.name}"
        self.router.add_api_route(
            path=f"/{self.view_meta.name}",
            endpoint=read_resources,
            methods=["GET"],
            response_model=List[self.pydantic_read_model],
            summary=f"Read and filter {self.view_meta.name} view records",
            description=self._generate_endpoint_description(),
            openapi_extra={"parameters": gen_openapi_parameters(self.view_meta)},
        )
