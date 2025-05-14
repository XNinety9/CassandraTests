"""
Entry point for running all replication tests against a Cassandra cluster.
"""
import argparse
import sys
import os
from typing import Callable, Dict, List, Tuple
from cassandra_utils.discovery import discover_nodes
from cassandra_utils.logger import log
from tests.sql_test import sql_test
from tests.nosql_test import nosql_test


def run_tests(
    tests: Dict[str, Callable[[List[Tuple[str, int]]], bool]],
    nodes: List[Tuple[str, int]],
    skip: List[str]
) -> bool:
    """
    Execute a series of replication tests by name.

    :param tests: Mapping of test names to functions.
    :param nodes: List of Cassandra node contact points.
    :param skip: List of test names to skip.
    :return: True if all executed tests passed.
    """
    overall_success = True
    for name, func in tests.items():
        if name in skip:
            log(f"[{name} skipped]")
            continue
        success = func(nodes)
        overall_success &= success
    return overall_success


def main() -> None:
    """
    Parse arguments, discover nodes, and run replication tests.

    Supports skipping specific tests via flags.
    """
    parser = argparse.ArgumentParser(
        description="Run Cassandra replication consistency tests."
    )
    parser.add_argument(
        "--skip", "-s",
        action="append",
        choices=["sql", "nosql"],
        help="Name of test to skip (can be repeated)."
    )
    args = parser.parse_args()

    # Discover Cassandra nodes
    nodes = discover_nodes()
    if len(nodes) < 2:
        sys.exit("✘ Need at least two Cassandra nodes running")

    log("Nodes under test: " + ", ".join(f"{h}:{p}" for h, p in nodes))

    # Map test keys to functions
    test_functions = {
        "sql": sql_test,
        "nosql": nosql_test,
    }
    # Merge CLI skips with environment variables
    skip_list = args.skip or []
    if os.getenv("SKIP_SQL"):
        skip_list.append("sql")
    if os.getenv("SKIP_NOSQL"):
        skip_list.append("nosql")

    all_passed = run_tests(test_functions, nodes, skip_list)
    if all_passed:
        log("ALL REPLICATION TESTS PASSED 🎉")
        sys.exit(0)
    log("ONE OR MORE TESTS FAILED ❌")
    sys.exit(1)


if __name__ == "__main__":
    main()
