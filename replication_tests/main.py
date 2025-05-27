import sys

from colorama import init as colorama_init
from colors import SUCCESS, FAIL, INFO, SEPARATOR
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
        log(FAIL + "❌ Need at least two Cassandra nodes running")
        sys.exit(1)

    log(f"[MAIN] Nodes under test: {', '.join(f'{h}:{p}' for h, p in nodes)}", INFO)

    # --- SQL Replication Test ---
    log("-" * 80, SEPARATOR, True)
    log("🛠️  [MAIN] Starting SQL Replication Test", INFO)
    sql_ok = sql_test(nodes)
    log(
        f"{'✅' if sql_ok else '❌'}  [MAIN] SQL Test {'PASSED 🎉' if sql_ok else 'FAILED ❌'}",
        SUCCESS if sql_ok else FAIL,
    )

    # --- NoSQL Replication Test ---
    log("-" * 80, SEPARATOR, True)
    log(INFO + "🔧  [MAIN] Starting NoSQL Replication Test")
    nosql_ok = nosql_test(nodes)
    log(
        f"{'✅' if nosql_ok else '❌'}  [MAIN] NoSQL Test {'PASSED 🎉' if nosql_ok else 'FAILED ❌'}",
        SUCCESS if nosql_ok else FAIL,
    )

    # --- Disconnect/Reconnect Test ---
    log("-" * 80, SEPARATOR, True)
    log(INFO + "🔌  [MAIN] Starting Disconnect/Reconnect Test")
    disconnect_ok = disconnect_reconnect_test(nodes)
    log(
        f"{'✅' if disconnect_ok else '❌'}  [MAIN] Disconnect/Reconnect Test {'PASSED 🎉' if disconnect_ok else 'FAILED ❌'}",
        SUCCESS if disconnect_ok else FAIL,
    )

    # --- Network-Limit Tests ---
    log("-" * 80, SEPARATOR, True)
    log(INFO + "🌐  [MAIN] Starting Network-Limit Test #1")
    network_ok = network_limit_test(
        nodes, rate="100kbit", burst="32kbit", latency="50ms", rows=500
    )
    log(
        f"{'✅' if network_ok else '❌'}  [MAIN] Network-Limit Test {'PASSED 🎉' if network_ok else 'FAILED ❌'}",
        SUCCESS if network_ok else FAIL,
    )

    log("-" * 80, SEPARATOR, True)
    log(INFO + "🌐  [MAIN] Starting Network-Limit Test #1")
    network_ok = network_limit_test(
        nodes, rate="10kbit", burst="3kbit", latency="100ms", rows=500
    )
    log(
        f"{'✅' if network_ok else '❌'}  [MAIN] Network-Limit Test {'PASSED 🎉' if network_ok else 'FAILED ❌'}",
        SUCCESS if network_ok else FAIL,
    )

    # --- Summary ---
    log("-" * 80, SEPARATOR, True)
    if all([sql_ok, nosql_ok, disconnect_ok, network_ok]):
        log("🥳  [MAIN] ALL REPLICATION TESTS PASSED 🎉", SUCCESS)
        sys.exit(0)

    log("😞  [MAIN] ONE OR MORE TESTS FAILED ❌", FAIL)
    sys.exit(1)


if __name__ == "__main__":
    main()
