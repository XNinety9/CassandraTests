"""
End-to-end SQL replication consistency tests using Cassandra.
"""
import uuid
import random
import string
import time
from typing import List, Tuple
from cassandra_utils.driver import make_session
from cassandra_utils.logger import log


def sql_test(nodes: List[Tuple[str, int]]) -> bool:
    """
    Run a simple key-value insert/read test across multiple Cassandra nodes.

    :param nodes: List of (host, port) tuples for cluster nodes.
    :return: True if all replication checks passed; False otherwise.
    """
    log("SQL TEST → creating sessions …")
    sessions = [make_session(h, p) for h, p in nodes]
    writer, readers = sessions[0], sessions[1:]

    keyspace = "replication_test"
    # Create keyspace with replication factor equal to number of nodes
    writer.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{len(nodes)}}}
        """
    )
    # Set keyspace on all sessions
    for session in sessions:
        session.set_keyspace(keyspace)
    # Create a simple table
    writer.execute("CREATE TABLE IF NOT EXISTS kv (id uuid PRIMARY KEY, value text)")

    # Prepare random test data
    row_id = uuid.uuid4()
    value = "".join(random.choices(string.ascii_letters + string.digits, k=12))
    log(f"SQL → INSERT id={row_id} val='{value}' via {nodes[0][0]}:{nodes[0][1]}")
    writer.execute(writer.prepare("INSERT INTO kv (id, value) VALUES (?, ?)") , (row_id, value))

    # Allow time for replication
    time.sleep(2)
    select_stmt = writer.prepare("SELECT value FROM kv WHERE id=?")
    success = True
    # Verify on all reader nodes
    for (host, port), session in zip(nodes[1:], readers):
        row = session.execute(select_stmt, (row_id,)).one()
        if not row or row.value != value:
            log(f"❌ SQL mismatch on {host}:{port}")
            success = False
        else:
            log(f"✅ SQL ok on {host}:{port}")
    # Clean up
    for session in sessions:
        session.cluster.shutdown()
    return success
