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
    payload_schema: dict | None = None  # For POST/PUT/PATCH

    # Instance variable to hold the generated data for a single run
    _payload: dict | None = None

    # In harness.py, inside the TestCase class

    def generate_payload(self, unique_suffix: str) -> None:
        """Generates a random but deterministically unique payload based on the schema."""
        if not self.payload_schema:
            self._payload = None
            return

        payload = {}
        for key, type_def in self.payload_schema.items():
            if type_def == "uuid":
                # We still generate a random UUID for the PK, as it must be unique globally.
                payload[key] = str(uuid.uuid4())
            elif type_def == "string":
                # The name is now deterministic and unique within the test run.
                payload[key] = f"test_{key}_{unique_suffix}"
            elif type_def == "email":
                payload[key] = f"test.{unique_suffix}@example.com"
            elif type_def == "int":
                # For other fields, random is still fine.
                payload[key] = random.randint(1, 100)
            elif type_def == "bool":
                payload[key] = random.choice([True, False])
            else:
                if isinstance(type_def, (str, int, bool, float)):
                    payload[key] = type_def
                else:
                    console.print(
                        f"[bold yellow]Warning:[/bold yellow] Unknown type keyword '{type_def}' for key '{key}' in payload schema. Skipping."
                    )

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
        endpoint="/infrastruct/faculty",  # A simple table to create records in
        payload_schema={
            "id": "uuid",
            "name": "string",
        },
    ),
    TestCase(
        name="Multi-PK - GET Record",
        method="GET",
        endpoint="/infrastruct/faculty_building",
    ),
    # TestCase(
    #     name="Multi-PK - DELETE Record",
    #     method="DELETE",
    #     endpoint="/account/user_roles",
    #     params={
    #         "user_id": "a3b1c4d5-e6f7-g8h9-i0j1-k2l3m4n5o6p7",
    #         "role_id": "r1s2t3u4-v5w6-x7y8-z9a0-b1c2d3e4f5g6",
    #     },
    # ),
    # TestCase(
    #     name="Validation - String Length (FAIL)",
    #     method="POST",
    #     endpoint="/infrastruct/faculty",
    #     payload_schema={
    #         "name": "a_very_long_string_that_should_definitely_exceed_the_varchar_limit_of_the_database_and_cause_a_validation_error"
    #     },
    # ),
]


# --- 3. The Test Runner ---

# In harness.py, replace the existing run_test_case function

# In harness.py, replace the entire run_test_case function


async def run_test_case(
    client: httpx.AsyncClient,
    base_url: str,
    case: TestCase,
    num_requests: int,
    concurrency: int,
) -> TestResult:
    """Runs a single test case concurrently and returns the result."""
    console.print(
        f"  -> Running [bold cyan]{case.name}[/bold cyan] against [bold]{base_url}[/bold]..."
    )

    # A counter specific to this single test run.
    request_counter = 0
    # A unique ID for this entire batch of requests.
    run_id = str(uuid.uuid4())[:8]

    async def single_request():
        nonlocal request_counter
        current_count = request_counter
        request_counter += 1

        # Create a guaranteed unique suffix for this specific request.
        unique_suffix = f"{run_id}_{current_count}"

        case.generate_payload(unique_suffix)
        final_payload = case._payload

        # Logic to handle prism-py's correct POST behavior
        if (
            "prism-py" in base_url
            and case.method == "POST"
            and final_payload
            and "id" in final_payload
        ):
            final_payload = final_payload.copy()
            del final_payload["id"]

        try:
            response = await client.request(
                method=case.method,
                url=f"{base_url}{case.endpoint}",
                params=case.params,
                json=final_payload,
                timeout=20,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            error_details = e.response.text[:200] if e.response else "No response body"
            raise RuntimeError(f"HTTP {e.response.status_code}: {error_details}")
        except Exception as e:
            raise RuntimeError(f"Request failed: {str(e)}")

    start_time = time.perf_counter()
    try:
        tasks = []
        for _ in range(num_requests):
            tasks.append(single_request())
            if len(tasks) >= concurrency:
                await asyncio.gather(*tasks)
                tasks = []
        if tasks:
            await asyncio.gather(*tasks)

        duration = time.perf_counter() - start_time
        return TestResult(
            test_name=case.name, target="", status="PASS", duration=duration
        )
    except Exception as e:
        duration = time.perf_counter() - start_time
        return TestResult(
            test_name=case.name,
            target="",
            status="FAIL",
            duration=duration,
            error=str(e),
        )


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
                forge_str = (
                    f"[green]{forge_res.duration:.4f}s[/]"
                    if forge_res.status == "PASS"
                    else f"[red]FAIL[/]"
                )

            prism_str = "[grey50]N/A[/]"
            if prism_res:
                prism_str = (
                    f"[green]{prism_res.duration:.4f}s[/]"
                    if prism_res.status == "PASS"
                    else f"[red]FAIL[/]"
                )

            winner_str = "[grey50]-[/]"
            if (
                forge_res
                and prism_res
                and forge_res.status == "PASS"
                and prism_res.status == "PASS"
            ):
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
# In harness.py, replace the entire main function with this new version


async def main(config: TestConfig):
    """Main function to run the entire test suite."""
    reporter = Reporter()
    console.rule("[bold]API Performance Test Harness[/bold]")
    console.print(
        f"Targeting {config.requests} requests with {config.concurrency} concurrency level."
    )

    # --- Test Execution ---
    try:
        async with httpx.AsyncClient() as client:
            for case in TEST_CASES:
                # Run for api-forge
                if "Advanced Filter" in case.name:
                    console.print(
                        f"  -> Skipping [bold cyan]{case.name}[/bold cyan] for api-forge (not supported)."
                    )
                else:
                    forge_result = await run_test_case(
                        client,
                        config.api_forge_url,
                        case,
                        config.requests,
                        config.concurrency,
                    )
                    reporter.add(forge_result, "api-forge")

                # Run for prism-py
                # --- This is the modification from the last step to handle the different POST payloads ---
                payload_backup = case._payload
                if (
                    "prism-py" in config.prism_py_url
                    and case.method == "POST"
                    and case._payload
                    and "id" in case._payload
                ):
                    final_payload = case._payload.copy()
                    del final_payload["id"]
                    case._payload = final_payload

                prism_result = await run_test_case(
                    client,
                    config.prism_py_url,
                    case,
                    config.requests,
                    config.concurrency,
                )
                reporter.add(prism_result, "prism-py")
                case._payload = (
                    payload_backup  # Restore payload for next test if needed
                )

                console.print("-" * 50)

    except Exception as e:
        console.print(
            f"\n[bold red]An unexpected error occurred during testing: {e}[/bold red]"
        )

    finally:
        # --- Reporting and Cleanup ---
        console.rule("[bold]Test Results[/bold]")
        reporter.print_summary()

        # Call the new cleanup function, passing the configuration
        await clean_db(config)


# --- 6. Database Cleanup Logic ---

# In harness.py, replace the existing _cleanup_target function


async def _cleanup_target(client: httpx.AsyncClient, target_name: str, base_url: str):
    """
    Finds and deletes all test records from a single API target.
    Handles the different DELETE syntaxes for api-forge and prism-py.
    """
    # This is the line we are fixing
    console.print(
        f"-> Checking for test records on [bold blue]{target_name}[/bold blue]..."
    )

    # Step 1: Get all records from the faculty table
    try:
        get_response = await client.get(
            f"{base_url}/infrastruct/faculty?limit=1000", timeout=10
        )
        get_response.raise_for_status()
        records = get_response.json()
    except Exception as e:
        console.print(
            f"  [red]Error:[/red] Could not fetch records from {target_name}. Cleanup aborted for this target. Reason: {e}"
        )
        return

    # Step 2: Filter to find records created by our test harness
    records_to_delete = [
        record
        for record in records
        if "name" in record
        and isinstance(record["name"], str)
        and record["name"].startswith("test_name_")
    ]

    if not records_to_delete:
        console.print("  [green]✓[/green] No test records found. Database is clean.")
        return

    console.print(
        f"  Found [yellow]{len(records_to_delete)}[/yellow] test records to delete."
    )

    # Step 3: Create concurrent DELETE requests
    delete_tasks = []
    for record in records_to_delete:
        record_id = record.get("id")
        if not record_id:
            continue

        if target_name == "prism-py":
            url = f"{base_url}/infrastruct/faculty/{record_id}"
            delete_tasks.append(client.delete(url, timeout=10))
        else:
            url = f"{base_url}/infrastruct/faculty"
            params = {"id": record_id}
            delete_tasks.append(client.delete(url, params=params, timeout=10))

    # Step 4: Execute all DELETE requests concurrently
    with console.status("[bold yellow]Deleting records...[/bold yellow]"):
        results = await asyncio.gather(*delete_tasks, return_exceptions=True)

    success_count = sum(
        1 for r in results if isinstance(r, httpx.Response) and r.is_success
    )
    fail_count = len(results) - success_count

    if fail_count > 0:
        console.print(f"  [red]Error:[/red] Failed to delete {fail_count} records.")

    console.print(
        f"  [green]✓[/green] Successfully deleted [bold]{success_count}[/bold] test records from {target_name}."
    )


async def clean_db(config: TestConfig):
    """
    Connects to both API servers and cleans up test-generated records.
    """
    async with httpx.AsyncClient() as client:
        console.rule("[bold yellow]Database Cleanup[/bold yellow]")
        await _cleanup_target(client, "api-forge", config.api_forge_url)
        console.print()  # Spacer line
        await _cleanup_target(client, "prism-py", config.prism_py_url)
        console.rule()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="API Performance and Stress Test Harness."
    )
    parser.add_argument(
        "--forge-url",
        default="http://localhost:7000",
        help="Base URL for the api-forge server.",
    )
    parser.add_argument(
        "--prism-url",
        default="http://localhost:8000",
        help="Base URL for the prism-py server.",
    )
    parser.add_argument(
        "-n",
        "--requests",
        type=int,
        default=100,
        help="Total number of requests to run per test.",
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=10,
        help="Number of concurrent requests to run in parallel.",
    )

    args = parser.parse_args()

    test_config = TestConfig(
        api_forge_url=args.forge_url,
        prism_py_url=args.prism_url,
        requests=args.requests,
        concurrency=args.concurrency,
    )

    asyncio.run(main(test_config))
