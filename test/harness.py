# harness.py
import httpx
import time
import asyncio
import argparse
import subprocess
import os
import random
import uuid
from rich.console import Console
from rich.table import Table
from dataclasses import dataclass, field

# --- 1. Configuration & Data Structures ---

@dataclass
class TestConfig:
    """Holds the configuration for the test run."""
    api_forge_url: str
    prism_py_url: str
    requests: int
    concurrency: int

@dataclass
class TestResult:
    """Holds the result of a single test execution."""
    test_name: str
    target: str  # "api-forge" or "prism-py"
    status: str  # "PASS" or "FAIL"
    duration: float
    error: str | None = None

@dataclass
class TestCase:
    """Defines a single test case to be run against both targets."""
    name: str
    method: str
    endpoint: str
    params: dict = field(default_factory=dict)
    payload_schema: dict | None = None # For POST/PUT/PATCH
    
    # Instance variable to hold the generated data for a single run
    _payload: dict | None = None

# In harness.py, inside the TestCase class

    def generate_payload(self) -> None:
        """Generates a random payload based on the schema."""
        if not self.payload_schema:
            self._payload = None
            return

        payload = {}
        for key, type_def in self.payload_schema.items():
            if type_def == "uuid":
                payload[key] = str(uuid.uuid4())
            elif type_def == "string":
                payload[key] = f"test_{key}_{random.randint(1000, 99999)}"
            elif type_def == "email":
                payload[key] = f"test.{random.randint(1000, 99999)}@example.com"
            elif type_def == "int":
                payload[key] = random.randint(1, 100)
            elif type_def == "bool":
                payload[key] = random.choice([True, False])
            # Add this 'else' block to catch unknown type definitions
            else:
                # If the type_def is a static value (not a keyword), use it directly
                if isinstance(type_def, (str, int, bool, float)):
                    payload[key] = type_def
                else:
                    console.print(f"[bold yellow]Warning:[/bold yellow] Unknown type keyword '{type_def}' for key '{key}' in payload schema. Skipping.")
        
        self._payload = payload

# --- 2. Test Definitions ---

# Define all your tests here. This makes adding new tests trivial.
TEST_CASES = [
    TestCase(
        name="GET - Metadata (Heavy)",
        method="GET",
        endpoint="/dt/schemas",
    ),
    TestCase(
        name="GET - Simple Table (Small)",
        method="GET",
        endpoint="/academic/course",
        params={"limit": 30},  # Limit to a reasonable number of records
    ),
    TestCase(
        name="GET - Advanced Filter",
        method="GET",
        endpoint="/academic/course_prerequisite",
        # params={"hire_date[gte]": "2022-01-01", "limit": 10},
    ),
    TestCase(
        name="POST - Create Record",
        method="POST",
        endpoint="/infrastruct/faculty", # A simple table to create records in
        payload_schema={
            "name": "string",
        },
    ),
    # Add PUT, PATCH, DELETE tests here later if needed
]


# --- 3. The Test Runner ---

async def run_test_case(client: httpx.AsyncClient, base_url: str, case: TestCase, num_requests: int, concurrency: int) -> TestResult:
    """Runs a single test case concurrently and returns the result."""
    console.print(f"  -> Running [bold cyan]{case.name}[/bold cyan] ({concurrency} concurrent requests)...")
    tasks = []
    
    async def single_request():
        # Generate a fresh payload for each request in a POST test
        if case.method in ["POST", "PUT", "PATCH"]:
            case.generate_payload()

        try:
            response = await client.request(
                method=case.method,
                url=f"{base_url}{case.endpoint}",
                params=case.params,
                json=case._payload,
                timeout=20,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            # Capture the response text if possible for better error reporting
            error_details = e.response.text[:200] if e.response else "No response body"
            raise RuntimeError(f"HTTP {e.response.status_code}: {error_details}")
        except Exception as e:
            raise RuntimeError(f"Request failed: {str(e)}")

    start_time = time.perf_counter()
    try:
        # Create concurrent batches
        for _ in range(num_requests // concurrency):
            batch = [single_request() for _ in range(concurrency)]
            await asyncio.gather(*batch)
        
        # Run any remaining requests
        remainder = num_requests % concurrency
        if remainder > 0:
            batch = [single_request() for _ in range(remainder)]
            await asyncio.gather(*batch)

        duration = time.perf_counter() - start_time
        return TestResult(test_name=case.name, target="", status="PASS", duration=duration)
    except Exception as e:
        duration = time.perf_counter() - start_time
        return TestResult(test_name=case.name, target="", status="FAIL", duration=duration, error=str(e))


# --- 4. Reporting ---

class Reporter:
    """Collects test results and prints a comparative summary."""
    def __init__(self):
        self.results: list[TestResult] = []

    def add(self, result: TestResult, target_name: str):
        result.target = target_name
        self.results.append(result)

    def print_summary(self):
        table = Table(title="Performance Test Suite Summary")
        table.add_column("Test Case", style="cyan", no_wrap=True)
        table.add_column("api-forge Result", justify="right")
        table.add_column("prism-py Result", justify="right")
        table.add_column("Winner", justify="center")

        grouped_results = {}
        for r in self.results:
            if r.test_name not in grouped_results:
                grouped_results[r.test_name] = {}
            grouped_results[r.test_name][r.target] = r

        for name, targets in grouped_results.items():
            forge_res = targets.get("api-forge")
            prism_res = targets.get("prism-py")

            forge_str = "[grey50]N/A[/]"
            if forge_res:
                forge_str = f"[green]{forge_res.duration:.4f}s[/]" if forge_res.status == "PASS" else f"[red]FAIL[/]"

            prism_str = "[grey50]N/A[/]"
            if prism_res:
                prism_str = f"[green]{prism_res.duration:.4f}s[/]" if prism_res.status == "PASS" else f"[red]FAIL[/]"

            winner_str = "[grey50]-[/]"
            if forge_res and prism_res and forge_res.status == "PASS" and prism_res.status == "PASS":
                if forge_res.duration < prism_res.duration:
                    winner_str = "[bold blue]api-forge[/]"
                else:
                    winner_str = "[bold magenta]prism-py[/]"
            elif forge_res and forge_res.status == "PASS":
                 winner_str = "[bold blue]api-forge[/]"
            elif prism_res and prism_res.status == "PASS":
                 winner_str = "[bold magenta]prism-py[/]"

            table.add_row(name, forge_str, prism_str, winner_str)
        
        console.print(table)


# --- 5. Main Execution Logic ---

console = Console()

async def main(config: TestConfig):
    """Main function to run the entire test suite."""
    reporter = Reporter()
    console.rule("[bold]API Performance Test Harness[/bold]")
    console.print(f"Targeting {config.requests} requests with {config.concurrency} concurrency level.")

    # Note: For POST tests to work, ensure the foreign key (faculty_id) exists in your db.
    # We will use a known static UUID for simplicity here.
    # In a real CI, you'd create this dependency first.
    fixed_faculty_id = "a1b2c3d4-e5f6-7777-8888-9999aabbccdd" # Replace with a real one from your DB
    for case in TEST_CASES:
        if case.payload_schema and "faculty_id" in case.payload_schema:
            case.payload_schema["faculty_id"] = fixed_faculty_id # Hack to use a valid FK

    async with httpx.AsyncClient() as client:
        for case in TEST_CASES:
            # Run for api-forge
            if "Advanced Filter" in case.name: # api-forge doesn't support this
                console.print(f"  -> Skipping [bold cyan]{case.name}[/bold cyan] for api-forge (not supported).")
            else:
                forge_result = await run_test_case(client, config.api_forge_url, case, config.requests, config.concurrency)
                reporter.add(forge_result, "api-forge")

            # Run for prism-py
            prism_result = await run_test_case(client, config.prism_py_url, case, config.requests, config.concurrency)
            reporter.add(prism_result, "prism-py")
            
            console.print("-" * 50)

    console.rule("[bold]Test Results[/bold]")
    reporter.print_summary()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="API Performance and Stress Test Harness.")
    parser.add_argument("--forge-url", default="http://localhost:7000", help="Base URL for the api-forge server.")
    parser.add_argument("--prism-url", default="http://localhost:8000", help="Base URL for the prism-py server.")
    parser.add_argument("-n", "--requests", type=int, default=100, help="Total number of requests to run per test.")
    parser.add_argument("-c", "--concurrency", type=int, default=10, help="Number of concurrent requests to run in parallel.")
    
    args = parser.parse_args()

    test_config = TestConfig(
        api_forge_url=args.forge_url,
        prism_py_url=args.prism_url,
        requests=args.requests,
        concurrency=args.concurrency
    )

    asyncio.run(main(test_config))