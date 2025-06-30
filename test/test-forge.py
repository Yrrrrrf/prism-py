import sys
from pathlib import Path

from fastapi import Response

# Add the server directory to Python path
server_dir = str(Path(__file__).parent.parent)
if server_dir not in sys.path: sys.path.append(server_dir)


from examples.main import app_forge
from fastapi.testclient import TestClient

# ? Test App ---------------------------------------------------------------------------------------

client = TestClient(app_forge.app)

def test_metadata():
    """Test the metadata endpoint"""
    response: Response = client.get("/dt/")

    assert response.status_code == 200
    
    print("\nMetadata Test Results:")
    print("Response status:", response.status_code)
    print("Metadata content:", response.json())


if __name__ == '__main__':
    test_metadata()

# TODO: Manage some way to handle the necessary test declarations on the '00-app.py' file
# * Then, import the test functions from that file to this one or any other test file...
# * Then, import the test functions from that file to this one or any other test file...
# * Then, import the test functions from that file to this one or any other test file...
