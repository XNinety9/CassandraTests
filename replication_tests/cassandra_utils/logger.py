# File: cassandra_utils/logger.py
"""
Simple incremental logger for numbered messages.
"""
# pylint: disable=global-statement
STEP = {"n": 0}


def log(msg: str) -> None:
    """
    Print a numbered log message to stdout and increment the counter.

    :param msg: Message to log.
    """
    STEP["n"] += 1
    print(f"[{STEP['n']:02}] {msg}", flush=True)