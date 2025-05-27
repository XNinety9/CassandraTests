# File: tests/network_limit_test.py
"""
Simulate limited network throughput between Cassandra nodes and measure replication speed.
"""
import json
import random
import subprocess
import string
import time
import uuid
from typing import List, Optional, Tuple

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


def network_limit_test(
    nodes: List[Tuple[str, int]],
    rate: str = "100kbit",
    burst: str = "32kbit",
    latency: str = "50ms",
    rows: int = 10,
    value_size: int = 10000
) -> bool:
    """
    Throttle egress on a random Cassandra node, insert rows, and measure replication time.
    """
    idx = random.randrange(len(nodes))
    host_thr, port_thr = nodes[idx]
    log(
        f"Throttling node: {INFO}{host_thr}:{port_thr}{RESET} at "
        f"{INFO}{rate}{RESET}, burst={INFO}{burst}{RESET}, latency={INFO}{latency}{RESET}",
        RESET
    )

    container_id = _get_container_id_for_port(port_thr)
    if not container_id:
        log(f"Could not find container for port {INFO}{port_thr}{RESET}", FAIL)
        return False

    try:
        subprocess.run([
            "podman", "exec", container_id,
            "tc", "qdisc", "add", "dev", "eth0", "root", "tbf",
            "rate", rate,
            "burst", burst,
            "latency", latency
        ], check=True)
        log("tc throttle applied successfully", RESET)
    except subprocess.CalledProcessError as err:
        log(f"Failed to apply tc throttle: {INFO}{err}{RESET}", FAIL)
        return False

    # Prepare writer session
    writer_index = 0 if idx != 0 else 1
    writer_host, writer_port = nodes[writer_index]
    writer_session = make_session(writer_host, writer_port)

    keyspace = "network_limit_test"

    # Ensure keyspace exists
    writer_session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class':'SimpleStrategy','replication_factor':{len(nodes)}}}
    """
    )
    writer_session.set_keyspace(keyspace)

    # Drop and recreate table cleanly
    writer_session.execute("DROP TABLE IF EXISTS kv")
    writer_session.execute("CREATE TABLE kv (id uuid PRIMARY KEY, value text)")

    log(f"Inserting {INFO}{rows}{RESET} rows into {INFO}{keyspace}.kv{RESET}...", RESET)
    for _ in range(rows):
        row_id = uuid.uuid4()
        payload = "".join(random.choices(string.ascii_letters + string.digits, k=value_size))
        writer_session.execute(
            writer_session.prepare("INSERT INTO kv (id, value) VALUES (?, ?)") ,
            (row_id, payload)
        )
    log(f"Inserted {INFO}{rows}{RESET} rows on {INFO}{writer_host}:{writer_port}{RESET}", RESET)

    # Start timing replication on throttled node
    start_time = time.time()
    reader_session = make_session(host_thr, port_thr)
    reader_session.set_keyspace(keyspace)

    timeout = rows * value_size / (int(rate.rstrip("kbit")) * 128) + 10
    elapsed = 0.0
    while elapsed < timeout:
        result = reader_session.execute("SELECT count(*) FROM kv").one()
        count = result[0] if result else 0
        if count >= rows:
            break
        time.sleep(1)
        elapsed = time.time() - start_time

    duration = time.time() - start_time
    throughput = rows / duration if duration > 0 else 0
    log(
        f"Replicated {INFO}{rows}{RESET} rows in {INFO}{duration:.2f}s{RESET} => "
        f"{INFO}{throughput:.2f}{RESET} rows/s",
        SUCCESS
    )

    # Clean up throttle
    try:
        subprocess.run([
            "podman", "exec", container_id,
            "tc", "qdisc", "del", "dev", "eth0", "root"
        ], check=True)
        log("tc throttle removed", RESET)
    except subprocess.CalledProcessError as err:
        log(f"Failed to remove tc qdisc: {INFO}{err}{RESET}", WARN)

    # Shutdown sessions
    writer_session.cluster.shutdown()
    reader_session.cluster.shutdown()
    return True
