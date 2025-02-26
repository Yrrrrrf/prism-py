"""Advanced logging utilities for the Forge framework."""

import re
import time
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any, Callable, Dict, Union, Optional


class ColorCode(str, Enum):
    """ANSI color and style codes."""

    # Styles
    RESET = "0"
    BOLD = "1"
    DIM = "2"
    ITALIC = "3"
    UNDERLINE = "4"
    BLINK = "5"
    REVERSE = "7"
    STRIKE = "9"

    # Colors (foreground)
    BLACK = "30"
    RED = "31"
    GREEN = "32"
    YELLOW = "33"
    BLUE = "34"
    MAGENTA = "35"
    CYAN = "36"
    WHITE = "37"

    # Bright colors
    BRIGHT_BLACK = "90"
    BRIGHT_RED = "91"
    BRIGHT_GREEN = "92"
    BRIGHT_YELLOW = "93"
    BRIGHT_BLUE = "94"
    BRIGHT_MAGENTA = "95"
    BRIGHT_CYAN = "96"
    BRIGHT_WHITE = "97"


# Color formatting function
def colorize(text: str, *codes: Union[ColorCode, str]) -> str:
    """Apply ANSI color/style codes to text."""
    if not codes:
        return text
    codes_str = ";".join(c.value if isinstance(c, ColorCode) else c for c in codes)
    return f"\033[{codes_str}m{text}\033[0m"


# Define optimized color functions using functools.lru_cache for efficiency
@lru_cache(maxsize=128)
def _color_fn(code: Union[ColorCode, str]) -> Callable[[str], str]:
    """Create a reusable color function for the given code."""
    return lambda text: colorize(text, code)


# Create color functions
bold = _color_fn(ColorCode.BOLD)
dim = _color_fn(ColorCode.DIM)
italic = _color_fn(ColorCode.ITALIC)
underline = _color_fn(ColorCode.UNDERLINE)
strike = _color_fn(ColorCode.STRIKE)

# Basic colors
black = _color_fn(ColorCode.BLACK)
# red = _color_fn(ColorCode.RED)
green = _color_fn(ColorCode.GREEN)
yellow = _color_fn(ColorCode.YELLOW)
blue = _color_fn(ColorCode.BLUE)
# magenta = _color_fn(ColorCode.MAGENTA)
cyan = _color_fn(ColorCode.CYAN)
white = _color_fn(ColorCode.WHITE)

# * Some specific colors...
# * RAINBOW COLORS
violet = lambda s: "\033[38;2;138;43;226m" + s + "\033[0m"
indigo = lambda s: "\033[38;2;75;0;130m" + s + "\033[0m"
# yellow = lambda s: "\033[38;2;255;255;0m" + s + "\033[0m"
orange = lambda s: "\033[38;2;255;165;0m" + s + "\033[0m"
red = lambda s: "\033[38;2;255;0;0m" + s + "\033[0m"
pink = lambda s: "\033[38;2;255;192;203m" + s + "\033[0m"


# Bright colors
bright_black = _color_fn(ColorCode.BRIGHT_BLACK)
bright_red = _color_fn(ColorCode.BRIGHT_RED)
bright_green = _color_fn(ColorCode.BRIGHT_GREEN)
bright_yellow = _color_fn(ColorCode.BRIGHT_YELLOW)
bright_blue = _color_fn(ColorCode.BRIGHT_BLUE)
bright_magenta = _color_fn(ColorCode.BRIGHT_MAGENTA)
bright_cyan = _color_fn(ColorCode.BRIGHT_CYAN)
bright_white = _color_fn(ColorCode.BRIGHT_WHITE)

# Combined styles (examples)
error_style = lambda text: colorize(text, ColorCode.BOLD, ColorCode.RED)
success_style = lambda text: colorize(text, ColorCode.BOLD, ColorCode.GREEN)
warning_style = lambda text: colorize(text, ColorCode.BOLD, ColorCode.YELLOW)
info_style = lambda text: colorize(text, ColorCode.BOLD, ColorCode.BLUE)

# Database element color palette (unified with TS implementation)
color_palette = {
    "table": blue,
    "view": green,
    "enum": violet,
    "function": red,
    "procedure": orange,
    "trigger": pink,
    "schema": bright_cyan,
    "column": lambda x: x,  # Default (no color)
    "pk": yellow,
    "fk": bright_blue,
    "type": dim,
    # * custom colors:
    # "trigger": lambda x: colorize(x, ColorCode.BRIGHT_MAGENTA, ColorCode.BOLD),
}


# Text utilities
def get_ansi_length(text: str) -> int:
    """Calculate visible length of string with ANSI codes removed."""
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return len(ansi_escape.sub("", text))


def pad_str(text: str, length: int, align: str = "left") -> str:
    """Pad string with spaces, accounting for ANSI color codes."""
    visible_length = get_ansi_length(text)
    padding = max(0, length - visible_length)

    match align:
        case "right":
            return " " * padding + text
        case "center":
            left_pad = padding // 2
            right_pad = padding - left_pad
            return " " * left_pad + text + " " * right_pad
        case _:  # Default is left-align
            return text + " " * padding


class LogLevel(str, Enum):
    """Log levels with associated colors."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    def get_color_fn(self) -> Callable[[str], str]:
        """Get the color function for this log level."""
        match self:
            case LogLevel.DEBUG:
                return lambda x: colorize(x, ColorCode.DIM, ColorCode.MAGENTA)
            case LogLevel.INFO:
                return lambda x: colorize(x, ColorCode.BLUE)
            case LogLevel.SUCCESS:
                return lambda x: colorize(x, ColorCode.GREEN)
            case LogLevel.WARNING:
                return lambda x: colorize(x, ColorCode.YELLOW)
            case LogLevel.ERROR:
                return lambda x: colorize(x, ColorCode.RED)
            case LogLevel.CRITICAL:
                return lambda x: colorize(x, ColorCode.BOLD, ColorCode.RED)


class Logger:
    """Enhanced logger with ANSI color support and timing utilities."""

    def __init__(self, module_name: str = "forge"):
        self.module_name = module_name
        self.indent_level = 0
        self.show_timestamp = True

    def _format_msg(self, level: LogLevel, message: str) -> str:
        """Format log message with consistent styling."""
        timestamp = (
            f"{dim(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))} "
            if self.show_timestamp
            else ""
        )
        indent = "  " * self.indent_level
        level_str = level.get_color_fn()(f"[{level.name}]")
        module_str = cyan(f"[{self.module_name}]")

        return f"{colorize(timestamp, ColorCode.ITALIC)}{level_str} {module_str} {indent}{message}"

    def log(self, level: LogLevel, message: str) -> None:
        """Log a message with the specified level."""
        print(self._format_msg(level, message))

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self.log(LogLevel.DEBUG, message)

    def info(self, message: str) -> None:
        """Log an info message."""
        self.log(LogLevel.INFO, message)

    def success(self, message: str) -> None:
        """Log a success message."""
        self.log(LogLevel.SUCCESS, message)

    def warning(self, message: str) -> None:
        """Log a warning message."""
        self.log(LogLevel.WARNING, message)

    def error(self, message: str) -> None:
        """Log an error message."""
        self.log(LogLevel.ERROR, message)

    def critical(self, message: str) -> None:
        """Log a critical error message."""
        self.log(LogLevel.CRITICAL, message)

    @contextmanager
    def indented(self, levels: int = 1):
        """Temporarily increase indentation for a block of code."""
        self.indent_level += levels
        try:
            yield
        finally:
            self.indent_level = max(0, self.indent_level - levels)

    @contextmanager
    def timed(self, operation: str = "Operation"):
        """Time an operation and log its duration."""
        start_time = time.time()
        self.info(f"Starting {operation}...")
        try:
            yield
        finally:
            elapsed = time.time() - start_time
            self.success(f"{operation} completed in {elapsed:.2f}s")

    def section(self, title: str) -> None:
        """Print a section header."""
        header = f"{'=' * 50}\n{title}\n{'=' * 50}"
        print(f"\n{bright_white(header)}")

    def table(self, headers: list, rows: list, widths: Optional[list] = None) -> None:
        """Print a formatted table with headers and rows."""
        if not widths:
            # Calculate widths based on content
            widths = [
                max(get_ansi_length(str(row[i])) for row in [headers] + rows)
                for i in range(len(headers))
            ]

        # Top border
        border_top = "┌" + "┬".join("─" * (w + 2) for w in widths) + "┐"
        print(border_top)

        # Header row
        header_cells = [
            pad_str(bright_white(str(h)), w) for h, w in zip(headers, widths)
        ]
        print("│ " + " │ ".join(header_cells) + " │")

        # Separator
        separator = "├" + "┼".join("─" * (w + 2) for w in widths) + "┤"
        print(separator)

        # Data rows
        for row in rows:
            cells = [pad_str(str(cell), w) for cell, w in zip(row, widths)]
            print("│ " + " │ ".join(cells) + " │")

        # Bottom border
        border_bottom = "└" + "┴".join("─" * (w + 2) for w in widths) + "┘"
        print(border_bottom)


# Create a shared logger instance
log: Logger = Logger()
