# File: main.py
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
from tests.disconnect_reconnect_test import disconnect_reconnect_test


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
            log(f"[MAIN] Skipping test: {name}")
            continue
        log(f"[MAIN] Running test: {name}")
        success = func(nodes)
        log(f"[MAIN] Test {name} {'PASSED' if success else 'FAILED'}")
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
        choices=["sql", "nosql", "disconnect"],
        help="Name of test to skip (can be repeated)."
    )
    args = parser.parse_args()

    nodes = discover_nodes()
    if len(nodes) < 2:
        sys.exit("✘ Need at least two Cassandra nodes running")

    log("[MAIN] Nodes under test: " + ", ".join(f"{h}:{p}" for h, p in nodes))

    test_functions: Dict[str, Callable[[List[Tuple[str, int]]], bool]] = {
        "sql": sql_test,
        "nosql": nosql_test,
        "disconnect": disconnect_reconnect_test,
    }

    skip_list = args.skip or []
    if os.getenv("SKIP_SQL"):
        skip_list.append("sql")
    if os.getenv("SKIP_NOSQL"):
        skip_list.append("nosql")
    if os.getenv("SKIP_DISCONNECT"):
        skip_list.append("disconnect")

    all_passed = run_tests(test_functions, nodes, skip_list)

    if all_passed:
        log("[MAIN] ALL REPLICATION TESTS PASSED 🎉")
        sys.exit(0)

    log("[MAIN] ONE OR MORE TESTS FAILED ❌")
    sys.exit(1)


if __name__ == "__main__":
    main()
