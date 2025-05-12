#!/usr/bin/env python3
"""
Multi‑node replication test using a NoSQL‑style `MAP<text,text>` document.
Same logic as host‑replication‑test.py but stores a Python dict into a map.
"""

import os, sys, time, uuid, json, random, string
from typing import List, Tuple

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel

step = 0

def log(msg):
    global step
    step += 1
    print(f"[{step:02}] {msg}", flush=True)

# Nodes

def default_nodes() -> List[str]:
    return [f"localhost:{9042 + i}" for i in range(4)]

raw_nodes = os.getenv("CASSANDRA_NODES", ",".join(default_nodes()))
node_specs: List[Tuple[str, int]] = [(h, int(p or 9042)) for h, _, p in (s.partition(":") for s in raw_nodes.split(","))]

if len(node_specs) < 2:
    sys.exit("Need at least two nodes")

log("Nodes: " + ", ".join(f"{h}:{p}" for h, p in node_specs))

# Profile helper

def profile_for(host):
    return ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=WhiteListRoundRobinPolicy([host])
    )

# Sessions
sessions = []
for host, port in node_specs:
    log(f"Connecting to {host}:{port}")
    sess = Cluster(contact_points=[host], port=port,
                   execution_profiles={EXEC_PROFILE_DEFAULT: profile_for(host)}).connect()
    sessions.append(sess)

writer, readers = sessions[0], sessions[1:]

# Schema
KS = "nosql_test"
writer.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KS}
    WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{len(sessions)}}}
""")
for s in sessions:
    s.set_keyspace(KS)
writer.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id  uuid PRIMARY KEY,
        doc map<text,text>
    )
""")

# Write dict
row_id = uuid.uuid4()
doc = {
    "user": random.choice(["alice", "bob", "carol", "dave"]),
    "score": str(random.randint(0, 100)),
    "token": "".join(random.choices(string.ascii_letters + string.digits, k=8))
}
log(f"INSERT doc id={row_id} → {json.dumps(doc)} via {node_specs[0][0]}:{node_specs[0][1]}")
ins = writer.prepare("INSERT INTO documents (id, doc) VALUES (?, ?)")
writer.execute(ins, (row_id, doc))

log("Sleeping 2 s for replication …")
time.sleep(2)

# Read from each reader
sel = readers[0].prepare("SELECT doc FROM documents WHERE id=?")
fail = []
for (host, port), sess in zip(node_specs[1:], readers):
    row = sess.execute(sel, (row_id,)).one()
    if not row or row.doc != doc:
        log(f"❌  {host}:{port} FAILED")
        fail.append(f"{host}:{port}")
    else:
        log(f"✅  {host}:{port} matched")

# Shutdown
for s in sessions:
    s.cluster.shutdown()

if fail:
    log("Replication FAILED on: " + ", ".join(fail))
    sys.exit(1)

log("Replication SUCCESS across all nodes 🎉")
sys.exit(0)
