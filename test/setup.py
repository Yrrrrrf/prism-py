import sys
from pathlib import Path

# ensure project root is on sys.path
root = Path(__file__).parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

from src.main import api_prism  # your FastAPI+Prism instance
# from prism import log, LogLevel

from fastapi.testclient import TestClient

client = TestClient(api_prism.app)
# log.set_level(LogLevel.NONE)
