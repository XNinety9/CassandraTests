# File: tests/disconnect_reconnect_test.py
"""
Simulate a node disconnect/reconnect and verify replication consistency.
"""
import json
import random
import subprocess
import time
import uuid
from typing import List, Optional, Tuple

from cassandra.cluster import OperationTimedOut
from cassandra_utils.driver import make_session
from cassandra_utils.logger import log
from colors import INFO, SUCCESS, FAIL, WARN, RESET

def _get_container_id_for_port(port: int) -> Optional[str]:
    """
    Return the Podman container ID exposing the given host port, or None if not found.
    """
    try:
        output = subprocess.check_output(
            ["podman", "ps", "--format", "json"], text=True, stderr=subprocess.DEVNULL
        )
        containers = json.loads(output)
    except subprocess.CalledProcessError:
        return None
    for cont in containers:
        for p in cont.get("Ports", []):
            if p.get("host_port") == port and p.get("protocol") == "tcp":
                return cont.get("Id")
    return None

def disconnect_reconnect_test(nodes: List[Tuple[str, int]]) -> bool:
    """
    Pause a random Cassandra node, write data elsewhere, then unpause and verify replication.
    """
    idx = random.randrange(len(nodes))
    host_to_pause, port_to_pause = nodes[idx]
    log(
        f"Selected node for pause: {INFO}{host_to_pause}:{port_to_pause}{RESET}",
        RESET
    )

    container_id = _get_container_id_for_port(port_to_pause)
    if not container_id:
        log(f"Could not find container for port {INFO}{port_to_pause}{RESET}", FAIL)
        return False
    log(f"Pausing container {INFO}{container_id}{RESET}", RESET)
    subprocess.run(["podman", "pause", container_id], check=True)

    writer_host, writer_port = random.choice([n for n in nodes if n[1] != port_to_pause])
    log(
        f"Writer node: {INFO}{writer_host}:{writer_port}{RESET}",
        RESET
    )
    writer_session = make_session(writer_host, writer_port)

    keyspace = "disconnect_test"
    try:
        writer_session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class':'SimpleStrategy','replication_factor':{len(nodes)}}}
        """)
    except OperationTimedOut:
        log("Timeout creating keyspace; retrying once...", WARN)
        try:
            writer_session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class':'SimpleStrategy','replication_factor':{len(nodes)}}}
            """)
        except OperationTimedOut:
            log("Failed to create keyspace after retry", FAIL)
            writer_session.cluster.shutdown()
            return False

    writer_session.set_keyspace(keyspace)

    create_table_cql = "CREATE TABLE IF NOT EXISTS kv (id uuid PRIMARY KEY, value text)"
    for attempt in range(2):
        try:
            writer_session.execute(create_table_cql)
            break
        except OperationTimedOut:
            log(f"Timeout creating table (attempt {attempt+1}); retrying...", WARN)
            time.sleep(1)
    else:
        log("Failed to create table after retries", FAIL)
        writer_session.cluster.shutdown()
        return False

    test_id = uuid.uuid4()
    test_value = "reconnect_" + str(random.randint(1000, 9999))
    log(
        f"Writing id={INFO}{test_id}{RESET} value={INFO}{test_value}{RESET} "
        f"on {INFO}{writer_host}:{writer_port}{RESET}",
        RESET
    )
    prepared = writer_session.prepare("INSERT INTO kv (id, value) VALUES (?, ?)")
    for attempt in range(2):
        try:
            writer_session.execute(prepared, (test_id, test_value))
            break
        except OperationTimedOut:
            log(f"Timeout inserting row (attempt {attempt+1}); retrying...", WARN)
            time.sleep(1)
    else:
        log("Failed to insert row after retries", FAIL)
        writer_session.cluster.shutdown()
        return False

    log(f"Unpausing container {INFO}{container_id}{RESET}", RESET)
    subprocess.run(["podman", "unpause", container_id], check=True)
    time.sleep(10)

    resumed_session = make_session(host_to_pause, port_to_pause)
    resumed_session.set_keyspace(keyspace)
    select_stmt = resumed_session.prepare("SELECT value FROM kv WHERE id=?")
    try:
        row = resumed_session.execute(select_stmt, (test_id,)).one()
    except OperationTimedOut:
        log("Timeout querying resumed node", FAIL)
        row = None

    writer_session.cluster.shutdown()
    resumed_session.cluster.shutdown()

    if not row or row.value != test_value:
        log(
            f"Mismatch after reconnect on {INFO}{host_to_pause}:{port_to_pause}{RESET}",
            FAIL
        )
        return False
    log(
        f"Reconnect test passed on {INFO}{host_to_pause}:{port_to_pause}{RESET}",
        SUCCESS
    )
    return True
