"""
test_enums.py

Enum members generated for request validation must bind to reflected enum columns.
No database needed.
"""

from sqlalchemy import Enum
from sqlalchemy.dialects import postgresql

from prism.core.models.enums import EnumInfo

VALUES = ["pending", "active", "archived"]


def test_enum_members_bind_to_reflected_enum_column():
    py_enum = EnumInfo(name="status", schema="public", values=VALUES).to_python_enum()
    # How automap reflects a Postgres enum column: string values, no enum class.
    column_type = Enum(*VALUES, name="status")
    bind = column_type.bind_processor(postgresql.dialect())

    assert bind(py_enum["active"]) == "active"
    assert py_enum("active") is py_enum["active"]
