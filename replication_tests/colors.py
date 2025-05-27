# File: colors.py
"""
Defines color constants for CLI output using colorama.
"""
from colorama import Fore, Style

# Success messages (green)
SUCCESS = Fore.GREEN + Style.BRIGHT
# Failure messages (red)
FAIL = Fore.RED + Style.BRIGHT
# Informational highlights (cyan)
INFO = Fore.CYAN + Style.BRIGHT
# Warnings/divider lines (yellow)
WARN = Fore.YELLOW + Style.BRIGHT
# Reset to default terminal style
RESET = Style.RESET_ALL
