# File: main.py
"""
Simple driver to run all Cassandra replication tests sequentially,
with clear sections, emojis, and colors.
"""
import sys
from colorama import init as colorama_init
from colors import SUCCESS, FAIL, INFO, WARN, RESET
from cassandra_utils.discovery import discover_nodes
from cassandra_utils.logger import log
from tests.sql_test import sql_test
from tests.nosql_test import nosql_test
from tests.disconnect_reconnect_test import disconnect_reconnect_test
from tests.network_limit_test import network_limit_test

# Initialize colorama
colorama_init(autoreset=True)

def main() -> None:
    # Discover Cassandra nodes
    nodes = discover_nodes()
    if len(nodes) < 2:
        log("Need at least two Cassandra nodes running", FAIL)
        sys.exit(1)

    log(f"Nodes under test: {', '.join(f'{h}:{p}' for h, p in nodes)}", INFO)

    # --- SQL Replication Test ---
    log("-" * 80, WARN)
    log("🛠️  Starting SQL Replication Test", INFO)
    sql_ok = sql_test(nodes)
    log(f"SQL Test {'PASSED 🎉' if sql_ok else 'FAILED ❌'}", SUCCESS if sql_ok else FAIL)

    # --- NoSQL Replication Test ---
    log("-" * 80, WARN)
    log("🔧  Starting NoSQL Replication Test", INFO)
    nosql_ok = nosql_test(nodes)
    log(f"NoSQL Test {'PASSED 🎉' if nosql_ok else 'FAILED ❌'}", SUCCESS if nosql_ok else FAIL)

    # --- Disconnect/Reconnect Test ---
    log("-" * 80, WARN)
    log("🔌  Starting Disconnect/Reconnect Test", INFO)
    disconnect_ok = disconnect_reconnect_test(nodes)
    log(f"Disconnect/Reconnect Test {'PASSED 🎉' if disconnect_ok else 'FAILED ❌'}",
        SUCCESS if disconnect_ok else FAIL)

    # --- Network-Limit Test ---
    log("-" * 80, WARN)
    log("🌐  Starting Network-Limit Test", INFO)
    network_ok = network_limit_test(nodes)
    log(f"Network-Limit Test {'PASSED 🎉' if network_ok else 'FAILED ❌'}",
        SUCCESS if network_ok else FAIL)

    # --- Summary ---
    log("=" * 80, INFO)
    all_ok = all([sql_ok, nosql_ok, disconnect_ok, network_ok])
    if all_ok:
        log("ALL REPLICATION TESTS PASSED 🎉", SUCCESS)
        sys.exit(0)

    log("ONE OR MORE TESTS FAILED ❌", FAIL)
    sys.exit(1)

if __name__ == "__main__":
    main()
