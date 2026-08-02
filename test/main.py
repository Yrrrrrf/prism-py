"""
test-main.py

Runner script that executes both health and metadata tests.
"""

from test_health import test_health
from test_metadata import test_metadata

from setup import client  # now imported after we've raised the log level

if __name__ == "__main__":
    test_metadata()
    test_health()
    print("🎯 All tests passed successfully!")
