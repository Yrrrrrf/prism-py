"""Configuration management for the Prism framework."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class PrismConfig:
    """Configuration for the Prism API framework.

    Attributes:
        project_name: The name of your project
        version: The version of your project
        description: A brief description of your project
        author: Author name
        email: Contact email
        license_info: License information dictionary with 'name' and 'url' keys
    """

    project_name: str
    version: str = "0.1.0"
    description: Optional[str] = None
    author: Optional[str] = None
    email: Optional[str] = None
    license_info: Dict[str, str] = field(
        default_factory=lambda: {
            "name": "MIT",
            "url": "https://choosealicense.com/licenses/mit/",
        }
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary for FastAPI metadata."""
        return {
            "title": self.project_name,
            "version": self.version,
            "description": self.description,
            "contact": {"name": self.author, "email": self.email}
            if self.author
            else None,
            "license_info": self.license_info if self.license_info else None,
        }
