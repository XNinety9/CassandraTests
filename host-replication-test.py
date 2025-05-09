#!/usr/bin/env python3
"""
host-replication-test.py  (verbose)
───────────────────────────────────
Checks that a two-node Cassandra cluster replicates data.

  • Writes through node A  → localhost:9042
  • Reads  through node B  → localhost:9043

All major steps log to stdout with a numeric prefix.

Environment overrides:
  CASSANDRA_SEED / CASSANDRA_SEED_PORT
  CASSANDRA_PEER / CASSANDRA_PEER_PORT
"""

import os, sys, time, uuid, random, string
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel

# ───────────────────────────────────────────────────────────────────
# Utility: print numbered steps
# ───────────────────────────────────────────────────────────────────
_step = 0
def log(msg: str):
    global _step
    _step += 1
    print(f"[{_step:02}] {msg}", flush=True)

# ───────────────────────────────────────────────────────────────────
# 1.  Parse connection targets (with defaults)
# ───────────────────────────────────────────────────────────────────
SEED_HOST  = os.getenv("CASSANDRA_SEED", "localhost")
SEED_PORT  = int(os.getenv("CASSANDRA_SEED_PORT", 9042))
PEER_HOST  = os.getenv("CASSANDRA_PEER", "localhost")
PEER_PORT  = int(os.getenv("CASSANDRA_PEER_PORT", 9043))

log(f"Seed node  → {SEED_HOST}:{SEED_PORT}")
log(f"Peer node  → {PEER_HOST}:{PEER_PORT}")

# ───────────────────────────────────────────────────────────────────
# 2.  Helper: profile pinned to one host, CL=ONE
# ───────────────────────────────────────────────────────────────────
def profile_for(hostname: str):
    return ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=WhiteListRoundRobinPolicy([hostname])
    )

# ───────────────────────────────────────────────────────────────────
# 3.  Open two sessions (one per node)
# ───────────────────────────────────────────────────────────────────
def new_session(host, port):
    profile = profile_for(host)
    log(f"Connecting to {host}:{port} …")
    return Cluster(
        contact_points=[host],
        port=port,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile}
    ).connect()

seed_sess = new_session(SEED_HOST, SEED_PORT)
peer_sess = new_session(PEER_HOST, PEER_PORT)

# ───────────────────────────────────────────────────────────────────
# 4.  Create keyspace + table if needed
# ───────────────────────────────────────────────────────────────────
KEYSPACE = "replication_test"
log(f"Ensuring keyspace '{KEYSPACE}' (RF=2)")
seed_sess.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class':'SimpleStrategy', 'replication_factor':2}}
""")

for s in (seed_sess, peer_sess):
    s.set_keyspace(KEYSPACE)

log("Ensuring table 'test_data'")
seed_sess.execute("""
    CREATE TABLE IF NOT EXISTS test_data (
        id    uuid PRIMARY KEY,
        value text
    )
""")

# ───────────────────────────────────────────────────────────────────
# 5.  Write a random row via the seed node
# ───────────────────────────────────────────────────────────────────
row_id = uuid.uuid4()
value  = "".join(random.choices(string.ascii_letters + string.digits, k=12))
log(f"Inserting row id={row_id} value='{value}'")

insert = seed_sess.prepare(
    "INSERT INTO test_data (id, value) VALUES (?, ?)"
)
seed_sess.execute(insert, (row_id, value))

# ───────────────────────────────────────────────────────────────────
# 6.  Pause briefly so replication can finish
# ───────────────────────────────────────────────────────────────────
log("Sleeping 2 s to let replicas catch up …")
time.sleep(2)

# ───────────────────────────────────────────────────────────────────
# 7.  Read the row via the peer node
# ───────────────────────────────────────────────────────────────────
log(f"Selecting row id={row_id} from peer")
select = peer_sess.prepare(
    "SELECT value FROM test_data WHERE id = ?"
)
row = peer_sess.execute(select, (row_id,)).one()

# ───────────────────────────────────────────────────────────────────
# 8.  Evaluate and report
# ───────────────────────────────────────────────────────────────────
if row and row.value == value:
    log("Replication SUCCESS ✅ — peer returned expected value")
    rc = 0
else:
    log("Replication FAILED ❌ — peer did not return expected value")
    rc = 1

# ───────────────────────────────────────────────────────────────────
# 9.  Clean shutdown
# ───────────────────────────────────────────────────────────────────
log("Shutting down driver sessions …")
seed_sess.cluster.shutdown()
peer_sess.cluster.shutdown()
log("Done.")
sys.exit(rc)
