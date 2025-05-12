# replication_tests.py – run **both** SQL and NoSQL replication checks sequentially
# ---------------------------------------------------------------------------
# Simply execute:
#     python replication_tests.py
# The script will:
#   1. **Discover Cassandra nodes** via one of three methods (prints which).
#   2. Run the SQL-style table replication test.
#   3. Run the NoSQL (MAP column) replication test.
#   4. Exit 0 only if **both** tests succeed.
#
# Optional env‑vars:
#   • CASSANDRA_NODES – comma‑sep host[:port] list to override discovery.
#   • SKIP_SQL / SKIP_NOSQL – set to any non‑empty value to skip that test.
#
# Requirements: cassandra-driver, Podman (for auto‑discovery).

import json
import os
import random
import re
import string
import subprocess
import sys
import time
import uuid
from typing import List, Tuple

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel

# ─────────────────────────── Discovery helpers ──────────────────────────────

def discover_nodes(env_var: str = "CASSANDRA_NODES", fallback_n: int = 4) -> List[Tuple[str, int]]:
    """Return a list of (host, port) tuples for Cassandra contact points.
    Prints which discovery method was chosen."""

    # 1 · manual override via env -----------------------------------------
    raw = os.getenv(env_var)
    if raw:
        print(f"[DISCOVERY] Using ${{{env_var}}} override → {raw}")
        return [
            (spec.partition(":")[0] or "localhost", int(spec.partition(":")[2] or 9042))
            for spec in raw.split(",")
        ]

    # 2 · auto-discover via Podman ---------------------------------------
    try:
        podman_json = subprocess.check_output(
            ["podman", "ps", "--format", "json"], text=True, stderr=subprocess.DEVNULL
        )
        containers = json.loads(podman_json)
    except Exception:
        containers = []

    nodes: List[Tuple[str, int]] = []
    str_port_re = re.compile(r"[\d.*]+:(\d+)->9042/tcp")

    for c in containers:
        names = c.get("Names") or c.get("Name") or ""
        cname = " ".join(names) if isinstance(names, list) else str(names)
        if "cassandra" not in cname.lower():
            continue
        ports = c.get("Ports") or []
        for entry in (ports if isinstance(ports, list) else [ports]):
            if entry.get("container_port") == 9042 and entry.get("protocol", "tcp") == "tcp":
                hp = int(entry.get("host_port", 0))
                if hp:
                    nodes.append(("localhost", hp))
                    break

    if nodes:
        print(f"[DISCOVERY] Found {len(nodes)} node(s) via Podman ps json")
        return nodes

    # 3 · fallback ---------------------------------------------------------
    print(f"[DISCOVERY] No containers found – falling back to localhost:9042‑{9042+fallback_n-1}")
    return [("localhost", 9042 + i) for i in range(fallback_n)]

# ─────────────────────────── Driver helper ─────────────────────────────────

def make_session(host: str, port: int):
    profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=WhiteListRoundRobinPolicy([host]),
    )
    return Cluster(contact_points=[host], port=port,
                   execution_profiles={EXEC_PROFILE_DEFAULT: profile}).connect()

# logging with incremental step numbers
step = {"n": 0}

def log(msg: str):
    step["n"] += 1
    print(f"[{step['n']:02}] {msg}", flush=True)

# ─────────────────────────── SQL test ─────────────────────────────────────

def sql_test(nodes: List[Tuple[str, int]]) -> bool:
    log("SQL TEST → creating sessions …")
    sessions = [make_session(h, p) for h, p in nodes]
    writer, readers = sessions[0], sessions[1:]

    KS = "replication_test"
    writer.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KS}
        WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{len(nodes)}}}
    """)
    for s in sessions:
        s.set_keyspace(KS)
    writer.execute("CREATE TABLE IF NOT EXISTS kv (id uuid PRIMARY KEY, value text)")

    row_id = uuid.uuid4()
    value = "".join(random.choices(string.ascii_letters + string.digits, k=12))
    log(f"SQL → INSERT id={row_id} val='{value}' via {nodes[0][0]}:{nodes[0][1]}")
    writer.execute(writer.prepare("INSERT INTO kv (id, value) VALUES (?, ?)") , (row_id, value))

    time.sleep(2)
    sel = writer.prepare("SELECT value FROM kv WHERE id=?")
    ok = True
    for (host, port), sess in zip(nodes[1:], readers):
        row = sess.execute(sel, (row_id,)).one()
        if not row or row.value != value:
            log(f"❌ SQL mismatch on {host}:{port}")
            ok = False
        else:
            log(f"✅ SQL ok on {host}:{port}")
    for s in sessions:
        s.cluster.shutdown()
    return ok

# ─────────────────────────── NoSQL test ─────────────────────────────────--

def nosql_test(nodes: List[Tuple[str, int]]) -> bool:
    log("NoSQL TEST → creating sessions …")
    sessions = [make_session(h, p) for h, p in nodes]
    writer, readers = sessions[0], sessions[1:]

    KS = "nosql_test"
    writer.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KS}
        WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{len(nodes)}}}
    """)
    for s in sessions:
        s.set_keyspace(KS)
    writer.execute("CREATE TABLE IF NOT EXISTS documents (id uuid PRIMARY KEY, doc map<text,text>)")

    row_id = uuid.uuid4()
    doc = {
        "user": random.choice(["alice", "bob", "carol", "dave"]),
        "score": str(random.randint(0, 100)),
        "token": "".join(random.choices(string.ascii_letters + string.digits, k=8)),
    }
    log(f"NoSQL → INSERT doc id={row_id} via {nodes[0][0]}:{nodes[0][1]}")
    writer.execute(writer.prepare("INSERT INTO documents (id, doc) VALUES (?, ?)"), (row_id, doc))

    time.sleep(2)
    sel = writer.prepare("SELECT doc FROM documents WHERE id=?")
    ok = True
    for (host, port), sess in zip(nodes[1:], readers):
        row = sess.execute(sel, (row_id,)).one()
        if not row or row.doc != doc:
            log(f"❌ NoSQL mismatch on {host}:{port}")
            ok = False
        else:
            log(f"✅ NoSQL ok on {host}:{port}")
    for s in sessions:
        s.cluster.shutdown()
    return ok

# ─────────────────────────── Main flow ─────────────────────────────────---

if __name__ == "__main__":
    all_nodes = discover_nodes()
    if len(all_nodes) < 2:
        sys.exit("✘ Need at least two Cassandra nodes running")
    log("Nodes under test: " + ", ".join(f"{h}:{p}" for h, p in all_nodes))

    overall_ok = True

    if not os.getenv("SKIP_SQL"):
        overall_ok &= sql_test(all_nodes)
    else:
        log("[SQL test skipped]")

    if not os.getenv("SKIP_NOSQL"):
        overall_ok &= nosql_test(all_nodes)
    else:
        log("[NoSQL test skipped]")

    if overall_ok:
        log("ALL REPLICATION TESTS PASSED 🎉")
        sys.exit(0)
    else:
        log("ONE OR MORE TESTS FAILED ❌")
        sys.exit(1)
