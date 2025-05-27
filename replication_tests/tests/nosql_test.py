# File: tests/nosql_test.py
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
from colors import INFO, SUCCESS, FAIL, RESET

def nosql_test(nodes: List[Tuple[str, int]]) -> bool:
    """
    Run a document (map<text,text>) insert/read test across multiple nodes.
    """
    log("Creating sessions…", RESET)
    sessions = [make_session(h, p) for h, p in nodes]
    writer, readers = sessions[0], sessions[1:]

    keyspace = "nosql_test"
    writer.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class':'SimpleStrategy','replication_factor':{len(nodes)}}}
    """)
    for session in sessions:
        session.set_keyspace(keyspace)
    writer.execute("CREATE TABLE IF NOT EXISTS documents (id uuid PRIMARY KEY, doc map<text,text>)")

    # Generate test document
    row_id = uuid.uuid4()
    doc = {
        "user": random.choice(["alice", "bob", "carol", "dave"]),
        "score": str(random.randint(0, 100)),
        "token": "".join(random.choices(string.ascii_letters + string.digits, k=8)),
    }
    log(
        f"INSERT doc id={INFO}{row_id}{RESET} via {INFO}{nodes[0][0]}:{nodes[0][1]}{RESET}",
        RESET
    )
    writer.execute(writer.prepare("INSERT INTO documents (id, doc) VALUES (?, ?)"), (row_id, doc))

    time.sleep(2)
    select_stmt = writer.prepare("SELECT doc FROM documents WHERE id=?")
    success = True
    for (host, port), session in zip(nodes[1:], readers):
        row = session.execute(select_stmt, (row_id,)).one()
        if not row or row.doc != doc:
            log(f"Mismatch on {INFO}{host}:{port}{RESET}", FAIL)
            success = False
        else:
            log(f"OK on {INFO}{host}:{port}{RESET}", SUCCESS)

    for session in sessions:
        session.cluster.shutdown()
    return success
