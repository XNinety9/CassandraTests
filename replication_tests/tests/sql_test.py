# File: tests/sql_test.py
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
from colors import INFO, SUCCESS, FAIL, RESET

def sql_test(nodes: List[Tuple[str, int]]) -> bool:
    """
    Run a simple key-value insert/read test across multiple Cassandra nodes.
    """
    log("Creating sessions…", RESET)
    sessions = [make_session(h, p) for h, p in nodes]
    writer, readers = sessions[0], sessions[1:]

    keyspace = "replication_test"
    writer.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class':'SimpleStrategy','replication_factor':{len(nodes)}}}
    """)
    for session in sessions:
        session.set_keyspace(keyspace)
    writer.execute("CREATE TABLE IF NOT EXISTS kv (id uuid PRIMARY KEY, value text)")

    # Prepare random test data
    row_id = uuid.uuid4()
    value = "".join(random.choices(string.ascii_letters + string.digits, k=12))
    log(
        f"INSERT id={INFO}{row_id}{RESET} val='{INFO}{value}{RESET}' via "
        f"{INFO}{nodes[0][0]}:{nodes[0][1]}{RESET}",
        RESET
    )
    writer.execute(writer.prepare("INSERT INTO kv (id, value) VALUES (?, ?)"), (row_id, value))

    time.sleep(2)
    select_stmt = writer.prepare("SELECT value FROM kv WHERE id=?")
    success = True
    for (host, port), session in zip(nodes[1:], readers):
        row = session.execute(select_stmt, (row_id,)).one()
        if not row or row.value != value:
            log(f"Mismatch on {INFO}{host}:{port}{RESET}", FAIL)
            success = False
        else:
            log(f"OK on {INFO}{host}:{port}{RESET}", SUCCESS)

    for session in sessions:
        session.cluster.shutdown()
    return success
