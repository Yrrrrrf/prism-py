# src/prism/api/routers/functions.py
import re
from typing import Any, Callable, Dict, List, Optional, Type, Union

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, create_model
from sqlalchemy import text
from sqlalchemy.orm import Session

from prism.core.models.functions import FunctionMetadata, FunctionType
from prism.core.types.utils import ArrayType, get_python_type
from prism.ui import console, display_function_structure


class BaseCallableGenerator:
    """Base class for items that can be called (functions/procedures)."""

    def __init__(
        self,
        metadata: FunctionMetadata,
        db_dependency: Callable[..., Session],
        router: APIRouter,
    ):
        self.meta = metadata
        self.db_dependency = db_dependency
        self.router = router
        self.input_model = self._create_input_model()

    def _create_input_model(self) -> Type[BaseModel]:
        """Dynamically creates a Pydantic model for the function's input parameters."""
        fields = {}
        for param in self.meta.parameters:
            if param.mode.upper() in ("OUT", "INOUT"):  # We only model input params
                continue

            py_type = get_python_type(param.type, nullable=param.has_default)
            final_type = (
                List[py_type.item_type] if isinstance(py_type, ArrayType) else py_type
            )

            # Pydantic needs a real default value for optional fields
            default = param.default_value if param.has_default else ...
            fields[param.name] = (final_type, default)

        return create_model(
            f"{self.meta.schema.capitalize()}{self.meta.name.capitalize()}Input",
            **fields,
            __config__=ConfigDict(from_attributes=True),
        )


class ProcedureGenerator(BaseCallableGenerator):
    """Generates a POST route to execute a database procedure."""

    def generate_routes(self):
        console.print(
            f"  -> Generating PROC route for: [cyan]{self.meta.schema}.[bold magenta]{self.meta.name}[/]"
        )
        display_function_structure(self.meta)

        def execute_procedure(
            params: self.input_model = Depends(),
            db: Session = Depends(self.db_dependency),
        ) -> Dict[str, str]:
            param_list = [f":{p}" for p in params.model_dump().keys()]
            query = f"CALL {self.meta.schema}.{self.meta.name}({', '.join(param_list)})"
            try:
                db.execute(text(query), params.model_dump())
                db.commit()
                return {
                    "status": "success",
                    "message": f"Procedure {self.meta.name} executed.",
                }
            except Exception as e:
                db.rollback()
                raise HTTPException(
                    status_code=500, detail=f"Procedure execution failed: {e}"
                )

        self.router.add_api_route(
            path=f"/proc/{self.meta.name}",
            endpoint=execute_procedure,
            methods=["POST"],
            summary=f"Execute procedure: {self.meta.name}",
            description=self.meta.description
            or f"Executes the `{self.meta.name}` procedure.",
        )


class FunctionGenerator(BaseCallableGenerator):
    """Generates a POST route to execute a database function."""

    def generate_routes(self):
        console.print(
            f"  -> Generating FUNC route for: [cyan]{self.meta.schema}.[bold red]{self.meta.name}[/]"
        )

        try:
            output_model = self._create_output_model()
        except ValueError as e:
            # If we can't create an output model, we can't generate a route.
            # Log a warning and skip this function.
            console.print(
                f"  🟡 Skipping function [bold magenta]{self.meta.name}[/]: Could not create output model. Reason: {e}"
            )
            return

        display_function_structure(self.meta)

        def execute_function(
            params: self.input_model = Depends(),
            db: Session = Depends(self.db_dependency),
        ) -> Any:
            param_list = [f":{p}" for p in params.model_dump().keys()]
            query = f"SELECT * FROM {self.meta.schema}.{self.meta.name}({', '.join(param_list)})"
            try:
                result = db.execute(text(query), params.model_dump())
                if self.meta.type == FunctionType.SCALAR:
                    # Return the first column of the first row
                    return result.scalar_one_or_none()
                else:  # TABLE or SET_RETURNING
                    return result.mappings().all()
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Function execution failed: {e}"
                )

        response_model = output_model
        if self.meta.type != FunctionType.SCALAR:
            response_model = List[output_model]

        self.router.add_api_route(
            path=f"/fn/{self.meta.name}",
            endpoint=execute_function,
            methods=["POST"],
            response_model=response_model,
            summary=f"Execute function: {self.meta.name}",
            description=self.meta.description
            or f"Executes the `{self.meta.name}` function.",
        )

    def _create_output_model(self) -> Type[BaseModel]:
        """Dynamically creates a Pydantic model for the function's return type."""
        fields = {}
        if self.meta.type == FunctionType.SCALAR:
            # For scalar, we can just return the raw type for the response model
            return_type = get_python_type(self.meta.return_type, nullable=True)
            return return_type
        else:  # TABLE or SET_RETURNING
            # Parse 'TABLE(col1 type1, col2 type2)'
            columns_str_match = re.search(r"\((.*?)\)", self.meta.return_type)
            if not columns_str_match:
                raise ValueError(
                    f"Return type '{self.meta.return_type}' is not a parseable TABLE type"
                )

            columns_str = columns_str_match.group(1)
            if not columns_str.strip():
                raise ValueError(
                    f"Return type '{self.meta.return_type}' is a TABLE type with no columns defined."
                )

            for column in columns_str.split(","):
                # Handle cases with or without explicit names like 'col_name int' vs 'int'
                parts = column.strip().split()
                if len(parts) >= 2:
                    col_name, col_type = parts[0], " ".join(parts[1:])
                else:
                    col_name, col_type = f"column_{len(fields)}", parts[0]

                py_type = get_python_type(col_type, nullable=True)
                fields[col_name] = (py_type, None)

        return create_model(
            f"{self.meta.schema.capitalize()}{self.meta.name.capitalize()}Output",
            **fields,
            __config__=ConfigDict(from_attributes=True),
        )


class TriggerGenerator:
    """This class doesn't generate routes, but logs discovered triggers for awareness."""

    def __init__(self, metadata: FunctionMetadata):
        self.meta = metadata

    def generate_routes(self):
        """This is a pseudo-generator; it just prints info."""
        console.print(
            f"  -> Discovered TRIGGER: [cyan]{self.meta.schema}.[bold orange1]{self.meta.name}[/]"
        )
        display_function_structure(self.meta)
