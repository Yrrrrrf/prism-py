"""
test-metadata.py

Smoke-test for /dt/schemas, with colored, compact logging per schema.
"""

# metadata subsections to check
from colorama import Fore, Style
from setup import client


SUBS = ["tables", "views", "enums", "functions", "procedures", "triggers"]


def _fmt_count(sub: str, cnt: int) -> str:
    """
    Return a compact colored string like 'tab:3' or 'vie:0'.
    Green if cnt>0, yellow if zero.
    """
    tag = sub[:3]  # e.g. 'tab' for 'tables'
    if cnt > 0:
        return f"{Fore.GREEN}{tag}:{cnt}{Style.RESET_ALL}"
    return f"{Fore.YELLOW}{tag}:0{Style.RESET_ALL}"


def test_metadata():
    """Fetch /dt/schemas, print summary, then conditionally hit non-empty endpoints."""
    # 1) Fetch all metadata at once
    resp = client.get("/dt/schemas")
    assert resp.status_code == 200, f"/dt/schemas → {resp.status_code}"
    all_meta = resp.json()

    # 2) Print a compact summary table per schema
    print(f"\n{Fore.CYAN}=== Metadata Summary ==={Style.RESET_ALL}")
    for schema in all_meta:
        name = schema["name"]
        # count items in each subsection
        counts = {sub: len(schema.get(sub) or {}) for sub in SUBS}
        # build a one-line summary
        summary = " ".join(_fmt_count(sub, counts[sub]) for sub in SUBS)
        print(f"  {Fore.BLUE}{name:<8}{Style.RESET_ALL} {summary}")
    print()

    # 3) Now hit each non-empty endpoint and log pass/fail
    for schema in all_meta:
        name = schema["name"]
        counts = {sub: len(schema.get(sub) or {}) for sub in SUBS}
        for sub in SUBS:
            if counts[sub] == 0:  # skip empty sections
                continue

            r = client.get(f"/dt/{name}/{sub}")
            status = (
                f"{Fore.GREEN}OK{Style.RESET_ALL}"
                if r.status_code == 200
                else f"{Fore.RED}FAIL {r.status_code}{Style.RESET_ALL}"
            )
            print(f" {status} → {name}/{sub:<10}")


if __name__ == "__main__":
    test_metadata()
    print(f"\n{Fore.CYAN}🎉 Conditional metadata tests completed!{Style.RESET_ALL}\n")
