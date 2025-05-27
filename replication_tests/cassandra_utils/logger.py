# File: cassandra_utils/logger.py
"""
Simple incremental logger that accepts a level parameter.
"""
from typing import Literal
from colors import INFO, SUCCESS, FAIL, WARN, RESET

LogLevel = Literal[INFO, SUCCESS, FAIL, WARN]

# Internal counter
_STEP = {"n": 0}

def log(message: str, level: str = INFO) -> None:
    """
    Print a numbered, colored log message.

    :param message: The text to log (emojis OK, no colors embedded).
    :param level:   Color/level constant (INFO, SUCCESS, FAIL, WARN).
    """
    _STEP["n"] += 1
    print(f"{level}[{_STEP['n']:02}] {message}{RESET}", flush=True)
