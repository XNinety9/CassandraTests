# --- file: host-replication-test.py (SQL style) -----------------------------
#!/usr/bin/env python3
"""
Multi‑node replication test (fixed‑schema table).

• Discovers up to 10 nodes from CASSANDRA_NODES env‑var or defaults to
  localhost:9042‑9045.
• Writes one row via the **first** node, then reads it back through **every**
  other node, proving cluster‑wide visibility.
• Exits 0 if every node returns the expected value, 1 otherwise.

Example:
    export CASSANDRA_NODES="localhost:9042,localhost:9043,localhost:9044,localhost:9045"
    python host-replication-test.py
"""

import os, sys, time, uuid, random, string
from typing import List, Tuple

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel

# ── helper: numbered logging ────────────────────────────────────────────
step = 0

def log(msg: str):
    global step
    step += 1
    print(f"[{step:02}] {msg}", flush=True)

# ── 1. gather node list ────────────────────────────────────────────────
def default_nodes() -> List[str]:
    return [f"localhost:{9042 + i}" for i in range(4)]  # 4‑node default

raw_nodes = os.getenv("CASSANDRA_NODES", ",".join(default_nodes()))
node_specs: List[Tuple[str, int]] = []
for spec in raw_nodes.split(","):
    host, _, port = spec.partition(":" )
    node_specs.append((host, int(port or 9042)))

if len(node_specs) < 2:
    sys.exit("Need at least two nodes to test replication")

log("Nodes discovered: " + ", ".join(f"{h}:{p}" for h, p in node_specs))

# ── 2. build profile factory ───────────────────────────────────────────

def profile_for(host):
    return ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=WhiteListRoundRobinPolicy([host]),
    )

# ── 3. open session per node ───────────────────────────────────────────
sessions = []
for host, port in node_specs:
    log(f"Connecting to {host}:{port}")
    prof = profile_for(host)
    sess = Cluster(contact_points=[host], port=port,
                   execution_profiles={EXEC_PROFILE_DEFAULT: prof}).connect()
    sessions.append(sess)

writer = sessions[0]
readers = sessions[1:]

# ── 4. schema (RF = number of nodes) ───────────────────────────────────
KS = "replication_test"
writer.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KS}
    WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{len(sessions)}}}
""")
for sess in sessions:
    sess.set_keyspace(KS)
writer.execute("""
    CREATE TABLE IF NOT EXISTS kv (
        id    uuid PRIMARY KEY,
        value text
    )
""")

# ── 5. write one row via first node ────────────────────────────────────
row_id = uuid.uuid4()
value = "".join(random.choices(string.ascii_letters + string.digits, k=12))
log(f"INSERT id={row_id} value='{value}' via {node_specs[0][0]}:{node_specs[0][1]}")
ins = writer.prepare("INSERT INTO kv (id, value) VALUES (?, ?)")
writer.execute(ins, (row_id, value))

log("Sleeping 2 s for replication …")
time.sleep(2)

# ── 6. read from every other node ──────────────────────────────────────
sel = sessions[1].prepare("SELECT value FROM kv WHERE id=?")  # prepared once, works everywhere
failures = []
for (host, port), sess in zip(node_specs[1:], readers):
    row = sess.execute(sel, (row_id,)).one()
    if not row or row.value != value:
        log(f"❌  {host}:{port} did NOT return expected value")
        failures.append(f"{host}:{port}")
    else:
        log(f"✅  {host}:{port} returned expected value")

# ── 7. summary + shutdown ─────────────────────────────────────────────
for s in sessions:
    s.cluster.shutdown()

if failures:
    log("Replication FAILED on: " + ", ".join(failures))
    sys.exit(1)

log("Replication SUCCESS across all nodes 🎉")
sys.exit(0)


