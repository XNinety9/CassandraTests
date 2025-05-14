"""
Map-column based document replication tests using Cassandra.
"""
import uuid
import random
import time
import string
from typing import List, Tuple
from cassandra_utils.driver import make_session
from cassandra_utils.logger import log


def nosql_test(nodes: List[Tuple[str, int]]) -> bool:
    """
    Run a document (map<text,text>) insert/read test across multiple nodes.

    :param nodes: List of (host, port) tuples for cluster nodes.
    :return: True if all replication checks passed; False otherwise.
    """
    log("NoSQL TEST → creating sessions …")
    sessions = [make_session(h, p) for h, p in nodes]
    writer, readers = sessions[0], sessions[1:]

    keyspace = "nosql_test"
    # Create keyspace with appropriate replication
    writer.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{len(nodes)}}}
        """
    )
    for session in sessions:
        session.set_keyspace(keyspace)
    # Create a table storing documents as maps
    writer.execute("CREATE TABLE IF NOT EXISTS documents (id uuid PRIMARY KEY, doc map<text,text>)")

    # Generate test document
    row_id = uuid.uuid4()
    doc = {
        "user": random.choice(["alice", "bob", "carol", "dave"]),
        "score": str(random.randint(0, 100)),
        "token": "".join(random.choices(string.ascii_letters + string.digits, k=8)),
    }
    log(f"NoSQL → INSERT doc id={row_id} via {nodes[0][0]}:{nodes[0][1]}")
    writer.execute(writer.prepare("INSERT INTO documents (id, doc) VALUES (?, ?)") , (row_id, doc))

    # Allow replication to settle
    time.sleep(2)
    select_stmt = writer.prepare("SELECT doc FROM documents WHERE id=?")
    success = True
    # Validate on each reader
    for (host, port), session in zip(nodes[1:], readers):
        row = session.execute(select_stmt, (row_id,)).one()
        if not row or row.doc != doc:
            log(f"❌ NoSQL mismatch on {host}:{port}")
            success = False
        else:
            log(f"✅ NoSQL ok on {host}:{port}")
    # Shutdown all sessions
    for session in sessions:
        session.cluster.shutdown()
    return success