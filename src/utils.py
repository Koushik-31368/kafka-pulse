"""
utils.py - Shared utilities for the Kafka-Pulse pipeline.

Provides:
  - get_db_config()       : build DB config dict from environment
  - get_db_connection()   : open a psycopg connection
  - retry()               : simple exponential-backoff retry decorator
  - fmt_ms()              : format a float as a human-readable ms/s string
  - SCHEMA_VERSION        : current pipeline schema version string
"""

import os
import time
import functools
import sys
import psycopg
from dotenv import load_dotenv

load_dotenv()

# -- Schema version ------------------------------------------------------------
SCHEMA_VERSION = "1.2.0"

# -- DB helpers ----------------------------------------------------------------

def get_db_config() -> dict:
    """Return a psycopg-compatible connection-kwarg dict from environment."""
    return {
        "host":     os.getenv("DB_HOST",     "localhost"),
        "port":     int(os.getenv("DB_PORT", "5432")),
        "dbname":   os.getenv("DB_NAME",     "pipeline_db"),
        "user":     os.getenv("DB_USER",     "pipeline_user"),
        "password": os.getenv("DB_PASSWORD", "pipeline_pass"),
    }


def get_db_connection(autocommit: bool = False) -> psycopg.Connection:
    """Open and return a psycopg connection. Raises psycopg.OperationalError on failure."""
    conn = psycopg.connect(**get_db_config())
    conn.autocommit = autocommit
    return conn


# -- Retry decorator -----------------------------------------------------------

def retry(
    exceptions: tuple = (Exception,),
    max_attempts: int = 3,
    base_delay: float = 1.0,
    backoff: float = 2.0,
    logger=None,
):
    """
    Decorator: retry *func* up to *max_attempts* times on *exceptions*.

    Delay between attempts grows as: base_delay * backoff^attempt

    Example
    -------
    @retry(exceptions=(psycopg.OperationalError,), max_attempts=5, base_delay=0.5)
    def flaky_db_call():
        ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        raise
                    msg = (
                        f"[retry] {func.__name__} failed (attempt {attempt}/{max_attempts}): "
                        f"{exc!r}. Retrying in {delay:.1f}s..."
                    )
                    if logger:
                        logger(msg)
                    else:
                        print(msg, file=sys.stderr)
                    time.sleep(delay)
                    delay *= backoff
        return wrapper
    return decorator


# -- Formatting helpers --------------------------------------------------------

def fmt_ms(value_ms: float | None, decimals: int = 2) -> str:
    """
    Format a millisecond value as a human-readable string.

      fmt_ms(3.14)      -> '3.14 ms'
      fmt_ms(1234.5)    -> '1.23 s'
      fmt_ms(None)      -> 'N/A'
    """
    if value_ms is None:
        return "N/A"
    if value_ms >= 1000:
        return f"{value_ms / 1000:.{decimals}f} s"
    return f"{value_ms:.{decimals}f} ms"


def fmt_eps(events_per_sec: float | None, decimals: int = 1) -> str:
    """Format an events-per-second figure."""
    if events_per_sec is None:
        return "N/A"
    if events_per_sec >= 1_000:
        return f"{events_per_sec / 1_000:.{decimals}f}k ev/s"
    return f"{events_per_sec:.{decimals}f} ev/s"


def highlight_latency(value_ms: float | None) -> str:
    """
    Return a Rich-markup coloured latency string.

      < 100 ms  -> green
      < 500 ms  -> yellow
      >= 500 ms -> red
    """
    if value_ms is None:
        return "[dim]N/A[/dim]"
    text = fmt_ms(value_ms)
    if value_ms < 100:
        return f"[green]{text}[/green]"
    if value_ms < 500:
        return f"[yellow]{text}[/yellow]"
    return f"[red]{text}[/red]"
