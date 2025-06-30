import httpx
import time
import asyncio

API_FORGE_BASE_URL = "http://localhost:7000"
PRISM_PY_BASE_URL = "http://localhost:8000"

# --- Test 1: Startup Time ---
# This must be done externally by scripting the launch of each server
# and polling the /health/ping endpoint until it responds.
# e.g., `time python -c 'import requests; while True: try: requests.get("http://localhost:8000/health/ping"); break; except: pass'`


async def run_request_test(client, url, num_requests=10):
    """Measures time to complete N sequential requests."""
    start_time = time.perf_counter()
    for _ in range(num_requests):
        await client.get(url)
    end_time = time.perf_counter()
    return end_time - start_time


async def main():
    async with httpx.AsyncClient() as client:
        print("--- Running Performance Tests ---")

        # --- Test 2: Simple GET (e.g., a small table like 'academic.faculty') ---
        url_forge = f"{API_FORGE_BASE_URL}/dt/schemas"
        url_prism = f"{PRISM_PY_BASE_URL}/dt/schemas"

        time_forge = await run_request_test(client, url_forge)
        time_prism = await run_request_test(client, url_prism)
        print(f"Simple GET (api-forge): {time_forge:.4f}s")
        print(f"Simple GET (prism-py):  {time_prism:.4f}s")

        # --- Test 3: Complex GET (only for prism-py) ---
        # This demonstrates a feature, not a direct performance comparison
        url_prism_complex = (
            f"{PRISM_PY_BASE_URL}/hr/employee?hire_date[gte]=2022-01-01&limit=10"
        )
        time_prism_complex = await run_request_test(client, url_prism_complex)
        print(f"Complex GET (prism-py): {time_prism_complex:.4f}s")

        # --- Test 4: POST Request ---
        # Use a table that doesn't have complex dependencies
        # (Note: You'd need to generate unique data for each POST)

    print("--- Tests Complete ---")


if __name__ == "__main__":
    asyncio.run(main())
