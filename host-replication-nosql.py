#!/usr/bin/env python3
"""
host-replication-nosql.py
─────────────────────────
Stores an unstructured document (Python dict → Cassandra MAP) on node A,
reads it on node B, and verifies replication.

Defaults:
  node A → localhost:9042   (seed)
  node B → localhost:9043   (peer)

Override with env-vars:
  CASSANDRA_SEED / CASSANDRA_SEED_PORT
  CASSANDRA_PEER / CASSANDRA_PEER_PORT
"""

import os, sys, time, uuid, json, random, string
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel

# ── console helper ──────────────────────────────────────────────────────
step = 0
def log(msg):
    global step
    step += 1
    print(f"[{step:02}] {msg}", flush=True)

# ── connection targets ──────────────────────────────────────────────────
A_HOST  = os.getenv("CASSANDRA_SEED", "localhost")
A_PORT  = int(os.getenv("CASSANDRA_SEED_PORT", 9042))
B_HOST  = os.getenv("CASSANDRA_PEER", "localhost")
B_PORT  = int(os.getenv("CASSANDRA_PEER_PORT", 9043))

log(f"Node A  → {A_HOST}:{A_PORT}  (write)")
log(f"Node B  → {B_HOST}:{B_PORT}  (read)")

# ── build execution profile: CL.ONE + pin to host ───────────────────────
def profile_for(host):
    return ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=WhiteListRoundRobinPolicy([host])
    )

def connect(host, port):
    profile = profile_for(host)
    log(f"Connecting to {host}:{port}")
    return Cluster(
        contact_points=[host],
        port=port,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile}
    ).connect()

sess_A = connect(A_HOST, A_PORT)   # writer
sess_B = connect(B_HOST, B_PORT)   # reader

# ── schema: keyspace + doc table with MAP column ───────────────────────
KS = "nosql_test"
TABLE = "documents"

log("Ensuring keyspace (RF=2)")
sess_A.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KS}
    WITH replication = {{'class':'SimpleStrategy', 'replication_factor':2}}
""")

for s in (sess_A, sess_B):
    s.set_keyspace(KS)

log("Ensuring table 'documents' (id UUID PK, doc MAP<text,text>)")
sess_A.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id  uuid PRIMARY KEY,
        doc map<text,text>
    )
""")

# ── craft a pseudo-JSON document and write on node A ────────────────────
doc_id = uuid.uuid4()
doc = {
    "user":   random.choice(["alice", "bob", "carol", "dave"]),
    "score":  str(random.randint(0, 100)),          # map values must be text
    "token":  "".join(random.choices(string.ascii_letters + string.digits, k=8))
}

log(f"Inserting doc id={doc_id} → {json.dumps(doc)}")
ins = sess_A.prepare(f"INSERT INTO {TABLE} (id, doc) VALUES (?, ?)")
sess_A.execute(ins, (doc_id, doc))     # driver maps Python dict → Cassandra map

# ── wait a bit so peer can replicate ────────────────────────────────────
log("Sleeping 2 s for replication …")
time.sleep(2)

# ── read back via node B ────────────────────────────────────────────────
log(f"Selecting doc id={doc_id} from node B")
sel = sess_B.prepare(f"SELECT doc FROM {TABLE} WHERE id = ?")
row = sess_B.execute(sel, (doc_id,)).one()

if row and row.doc == doc:
    log("Replication SUCCESS ✅ — document matched")
    rc = 0
else:
    log("Replication FAILED ❌ — document missing or mismatched")
    rc = 1

# ── tidy up ─────────────────────────────────────────────────────────────
log("Shutting down driver sessions …")
sess_A.cluster.shutdown()
sess_B.cluster.shutdown()
log("Done.")
sys.exit(rc)
