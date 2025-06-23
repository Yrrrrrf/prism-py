# src/prism/api/routers/crud.py
from typing import Any, Callable, List, Optional, Type

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, create_model
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from ...core.models.tables import TableMetadata
from ...core.types.utils import ArrayType, JSONBType, get_python_type


class CrudGenerator:
    """Generates CRUD API routes for a given table."""

    # ... __init__, generate_routes, and _add_read_route methods remain the same ...
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
        self.pydantic_model = self._create_pydantic_model() if self.sqlalchemy_model else None

    def generate_routes(self):
        if not self.sqlalchemy_model or not self.pydantic_model:
            return

        self._add_read_route()
        print(f"  ✓ Generated READ route for {self.table_meta.schema}.{self.table_meta.name}")

    def _add_read_route(self):
        sqlalchemy_model_local = self.sqlalchemy_model

        @self.router.get(
            f"/{self.table_meta.name}",
            response_model=List[self.pydantic_model],
            summary=f"Read {self.table_meta.name} records"
        )
        def read_resources(
            db: Session = Depends(self.db_dependency),
        ) -> List[Any]:
            results = db.query(sqlalchemy_model_local).all()
            return results

    def _create_pydantic_model(self) -> Type[BaseModel]:
        fields = {}
        for col in self.table_meta.columns:
            internal_type = get_python_type(col.sql_type, col.is_nullable)
            
            pydantic_type: Type
            if isinstance(internal_type, JSONBType):
                pydantic_type = Any
            elif isinstance(internal_type, ArrayType):
                if isinstance(internal_type.item_type, JSONBType):
                     pydantic_type = List[Any]
                else:
                     pydantic_type = List[internal_type.item_type]
            else:
                pydantic_type = internal_type

            # =================== THE FIX IS HERE ===================
            # Use the correct syntax for optional types.
            # `pydantic_type | None` is the modern way to say `Optional[pydantic_type]`.
            # We must NOT wrap this in `Type[...]`.
            final_type = pydantic_type
            if col.is_nullable:
                final_type = pydantic_type | None
            # =======================================================

            fields[col.name] = (final_type, ... if not col.is_nullable else None)

        return create_model(
            f"{self.table_meta.name.capitalize()}Model",
            **fields,
            __config__=ConfigDict(from_attributes=True)
        )
        
    def _get_sqlalchemy_model(self) -> Optional[Type]:
        # ... this method remains the same ...
        Base = automap_base()
        try:
            Base.prepare(self.engine, reflect=True, schema=self.table_meta.schema)
            model_class = getattr(Base.classes, self.table_meta.name, None)
            
            if model_class is None:
                print(f"  🟡 Skipping table {self.table_meta.schema}.{self.table_meta.name}: Could not automap. (Likely missing a primary key).")
                return None
                
            return model_class
        except Exception as e:
            print(f"  ❌ Skipping table {self.table_meta.schema}.{self.table_meta.name}: An unexpected error occurred during automap: {e}")
            return None