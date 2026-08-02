#!/usr/bin/env python3
"""
health_tester.py

Smoke-test for all /health endpoints, with colored output.
"""

from colorama import Fore, Style
from setup import client  # now imported after we've raised the log level


def test_health():
    """Hit each health route and print pass/fail in color."""
    print(f"\n{Fore.CYAN}=== Health Check ==={Style.RESET_ALL}")
    for method, path in [
        ("GET", "/health"),
        ("GET", "/health/ping"),
        ("GET", "/health/cache"),
    ]:
        resp = client.get(path)
        status = (
            f"{Fore.GREEN}OK{Style.RESET_ALL}"
            if resp.status_code == 200
            else f"{Fore.RED}FAIL {resp.status_code}{Style.RESET_ALL}"
        )
        print(f" {status} → {method:<3} {path:<20}")


if __name__ == "__main__":
    test_health()
    print(f"\n{Fore.CYAN}🎉 Health tests completed!{Style.RESET_ALL}\n")
