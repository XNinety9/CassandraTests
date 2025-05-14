"""
Discover Cassandra nodes via environment variable or Podman, with fallback to localhost.
"""
import os
import json
import subprocess
from typing import List, Tuple


def discover_nodes(env_var: str = "CASSANDRA_NODES", fallback_n: int = 4) -> List[Tuple[str, int]]:
    """
    Return a list of (host, port) tuples for Cassandra contact points.

    First tries an environment variable override, then Podman containers, then falls back.

    :param env_var: Name of the env var containing comma-separated host:port specs.
    :param fallback_n: Number of sequential localhost ports to use if no containers found.
    :return: List of host/port pairs.
    """
    override = os.getenv(env_var)
    if override:
        print(f"[DISCOVERY] Using ${{{env_var}}} override → {override}")
        # Parse comma-separated host:port entries
        return [
            (spec.partition(":")[0] or "localhost", int(spec.partition(":")[2] or 9042))
            for spec in override.split(",")
        ]

    try:
        # Query Podman for running containers in JSON
        podman_output = subprocess.check_output(
            ["podman", "ps", "--format", "json"], text=True, stderr=subprocess.DEVNULL
        )
        containers = json.loads(podman_output)
    except subprocess.CalledProcessError:
        containers = []  # Podman not available or error

    nodes: List[Tuple[str, int]] = []
    for container in containers:
        # Get container name(s)
        names = container.get("Names") or container.get("Name") or ""
        cname = " ".join(names) if isinstance(names, list) else str(names)
        if "cassandra" not in cname.lower():
            continue
        # Inspect ports for Cassandra default port
        for port_info in container.get("Ports", []):
            if (
                port_info.get("container_port") == 9042
                and port_info.get("protocol") == "tcp"
            ):
                host_port = port_info.get("host_port", 0)
                if host_port:
                    nodes.append(("localhost", host_port))
                    break

    if nodes:
        print(f"[DISCOVERY] Found {len(nodes)} node(s) via Podman ps json")
        return nodes

    # Fallback to sequential localhost ports
    start, end = 9042, 9042 + fallback_n - 1
    print(f"[DISCOVERY] No containers found – falling back to localhost:{start}-{end}")
    return [("localhost", start + i) for i in range(fallback_n)]
