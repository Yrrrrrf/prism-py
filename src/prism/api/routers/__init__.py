from typing import Any, Dict, List
from prism.core.types.utils import (
    PY_TO_JSON_SCHEMA_TYPE,
    ArrayType,
    JSONBType,
    get_python_type,
)


def gen_openapi_parameters(table_metadata) -> List[Dict[str, Any]]:
    parameters = []
    for col in table_metadata.columns:
        base_py_type = get_python_type(col.sql_type, nullable=False)
        if isinstance(base_py_type, (ArrayType, JSONBType)):
            continue
        json_type = PY_TO_JSON_SCHEMA_TYPE.get(base_py_type, "string")
        parameters.append(
            {
                "name": col.name,
                "in": "query",
                "required": False,
                "description": f"Filter records by an exact match on the '{col.name}' field.",
                "schema": {"type": json_type},
            }
        )
    parameters += [
        {
            "name": "limit",
            "in": "query",
            "required": False,
            "description": "Maximum number of records to return.",
            "schema": {"type": "integer", "default": 100},
        },
        {
            "name": "offset",
            "in": "query",
            "required": False,
            "description": "Number of records to skip.",
            "schema": {"type": "integer", "default": 0},
        },
        {
            "name": "order_by",
            "in": "query",
            "required": False,
            "description": "Column to sort by.",
            "schema": {"type": "string"},
        },
        {
            "name": "order_dir",
            "in": "query",
            "required": False,
            "description": "Sort direction: 'asc' or 'desc'.",
            "schema": {"type": "string", "default": "asc", "enum": ["asc", "desc"]},
        },
    ]
    return parameters
