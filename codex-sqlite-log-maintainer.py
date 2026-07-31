#!/usr/bin/env python3
"""Bounded, cooperative maintenance for a Codex SQLite log database.

This helper intentionally has no dependency outside Python's standard library.
It only returns small JSON metadata; it never reads or emits log contents.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import stat
import sys
import time
from typing import Any
from urllib.parse import quote


MAX_ERROR_CHARS = 240
LOG_DATABASE_NAME = re.compile(r"^logs[A-Za-z0-9_.-]*\.sqlite$")
MIN_UNIX_SECONDS = 946684800  # 2000-01-01; rejects millisecond/nanosecond schemas.
MAX_UNIX_SECONDS = 4102444800  # 2100-01-01.


class ValidationError(Exception):
    """A safe validation error suitable for returning to the dashboard."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def limited_error(error: BaseException | str) -> str:
    value = str(error).replace("\n", " ").replace("\r", " ").strip()
    return value[:MAX_ERROR_CHARS] or "unknown error"


def emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def current_codex_root() -> str:
    return os.path.realpath(os.path.abspath(os.path.expanduser("~/.codex")))


def is_inside(child: str, parent: str) -> bool:
    try:
        return os.path.commonpath([child, parent]) == parent
    except ValueError:
        return False


def checked_database_path(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValidationError("missing_path", "database path is required")
    if "\x00" in raw:
        raise ValidationError("invalid_path", "database path contains an invalid character")
    if raw.startswith("\\\\") or "\\" in raw or not os.path.isabs(raw):
        raise ValidationError(
            "invalid_path",
            "use an absolute WSL path such as /root/.codex/logs_2.sqlite",
        )

    candidate = os.path.abspath(os.path.normpath(raw))
    canonical = os.path.realpath(candidate)
    allowed_root = current_codex_root()
    if candidate != canonical:
        raise ValidationError("invalid_path", "database path must not traverse a symbolic link")
    if not is_inside(canonical, allowed_root):
        raise ValidationError("invalid_path", "database must be inside the current user's ~/.codex directory")
    if not LOG_DATABASE_NAME.fullmatch(os.path.basename(canonical)):
        raise ValidationError("invalid_path", "database file name must match logs*.sqlite")
    if canonical.endswith("-wal") or canonical.endswith("-shm"):
        raise ValidationError("invalid_path", "select the main .sqlite database, not a WAL or SHM sidecar")

    try:
        details = os.lstat(canonical)
    except FileNotFoundError as error:
        raise ValidationError("not_found", "database file does not exist") from error
    except OSError as error:
        raise ValidationError("unavailable", "database file cannot be inspected") from error
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise ValidationError("invalid_path", "database must be a regular non-symbolic-link file")
    return canonical


def regular_file_size(file_path: str, required: bool = False) -> int:
    try:
        details = os.lstat(file_path)
    except FileNotFoundError:
        if required:
            raise ValidationError("not_found", "database file does not exist")
        return 0
    except OSError as error:
        raise ValidationError("unavailable", "database file cannot be inspected") from error
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise ValidationError("invalid_sidecar", "database and SQLite sidecar files must be regular files")
    return max(0, int(details.st_size))


def storage_sizes(database_path: str) -> dict[str, int | float]:
    database_bytes = regular_file_size(database_path, required=True)
    wal_bytes = regular_file_size(database_path + "-wal")
    # Do not size or modify SHM, but refuse an unexpected special/symlink file
    # before asking SQLite to open a database that may use it.
    regular_file_size(database_path + "-shm")
    total_bytes = database_bytes + wal_bytes
    return {
        "databaseBytes": database_bytes,
        "walBytes": wal_bytes,
        "totalBytes": total_bytes,
        "totalMiB": round(total_bytes / (1024 * 1024), 2),
    }


def readonly_uri(database_path: str) -> str:
    return "file:" + quote(database_path, safe="/") + "?mode=ro"


def is_busy_error(error: BaseException) -> bool:
    message = str(error).lower()
    return "database is locked" in message or "database is busy" in message or "locked" in message


def validate_schema(database_path: str, busy_timeout_ms: int = 1000) -> None:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            readonly_uri(database_path),
            uri=True,
            timeout=max(0.0, busy_timeout_ms / 1000.0),
        )
        connection.execute("PRAGMA busy_timeout = %d" % max(0, busy_timeout_ms))
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'logs'"
        ).fetchone()
        if not row:
            raise ValidationError("invalid_schema", "SQLite database does not contain the expected logs table")
        columns = {
            str(item[1]).lower(): str(item[2] or "").upper()
            for item in connection.execute("PRAGMA table_info(logs)").fetchall()
        }
        if "id" not in columns or "ts" not in columns:
            raise ValidationError("invalid_schema", "logs table must contain id and ts columns")
        if "INT" not in columns["id"] or "INT" not in columns["ts"]:
            raise ValidationError("invalid_schema", "logs.id and logs.ts must use integer timestamps/identifiers")
        samples = connection.execute(
            "SELECT ts FROM logs WHERE ts IS NOT NULL ORDER BY id DESC LIMIT 8"
        ).fetchall()
        for (timestamp,) in samples:
            if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
                raise ValidationError("invalid_schema", "logs.ts is not a Unix-seconds timestamp")
            if not math.isfinite(float(timestamp)) or not (MIN_UNIX_SECONDS <= float(timestamp) <= MAX_UNIX_SECONDS):
                raise ValidationError("invalid_schema", "logs.ts must use Unix seconds rather than another timestamp unit")
    except ValidationError:
        raise
    except sqlite3.Error as error:
        if is_busy_error(error):
            raise ValidationError("database_busy", "SQLite database is busy; retry shortly") from error
        raise ValidationError("invalid_sqlite", "SQLite database cannot be opened or queried safely") from error
    finally:
        if connection is not None:
            connection.close()


def inspect_database(database_path: str, busy_timeout_ms: int = 1000) -> tuple[str, dict[str, int | float]]:
    checked_path = checked_database_path(database_path)
    sizes = storage_sizes(checked_path)
    validate_schema(checked_path, busy_timeout_ms)
    return checked_path, sizes


def check(database_path: str, busy_timeout_ms: int) -> dict[str, Any]:
    _, sizes = inspect_database(database_path, busy_timeout_ms)
    return {"ok": True, "result": "valid", **sizes}


def bounded_integer(value: int, minimum: int, maximum: int, name: str) -> int:
    parsed = int(value)
    if parsed < minimum or parsed > maximum:
        raise ValidationError("invalid_option", "%s must be between %d and %d" % (name, minimum, maximum))
    return parsed


def cleanup(
    database_path: str,
    threshold_mib: int,
    retain_hours: int,
    batch_rows: int,
    max_batches: int,
    busy_timeout_ms: int,
) -> dict[str, Any]:
    threshold_mib = bounded_integer(threshold_mib, 1, 102400, "threshold-mib")
    retain_hours = bounded_integer(retain_hours, 1, 8760, "retain-hours")
    batch_rows = bounded_integer(batch_rows, 1, 10000, "batch-rows")
    max_batches = bounded_integer(max_batches, 1, 20, "max-batches")
    busy_timeout_ms = bounded_integer(busy_timeout_ms, 0, 5000, "busy-timeout-ms")

    threshold_bytes = threshold_mib * 1024 * 1024
    cutoff_ts = int(time.time()) - retain_hours * 3600
    try:
        checked_path, before = inspect_database(database_path, busy_timeout_ms)
    except ValidationError as error:
        if error.code != "database_busy":
            raise
        # A read-only schema inspection can also be blocked by a legacy
        # rollback-journal writer. Treat that exactly like a blocked cleanup:
        # collect only file metadata and let the next scheduled pass retry.
        checked_path = checked_database_path(database_path)
        before = storage_sizes(checked_path)
        return {
            "ok": True,
            "result": "skipped_busy",
            "thresholdMiB": threshold_mib,
            "retainHours": retain_hours,
            "cutoffTs": cutoff_ts,
            "deletedRows": 0,
            "batches": 0,
            "physicalBytesBefore": before["totalBytes"],
            "physicalBytesAfter": before["totalBytes"],
            "physicalBytesDelta": 0,
            **before,
        }

    base = {
        "ok": True,
        "thresholdMiB": threshold_mib,
        "retainHours": retain_hours,
        "cutoffTs": cutoff_ts,
        "deletedRows": 0,
        "batches": 0,
        "physicalBytesBefore": before["totalBytes"],
        **before,
    }
    if before["totalBytes"] < threshold_bytes:
        return {**base, "result": "below_threshold", "physicalBytesAfter": before["totalBytes"]}

    connection: sqlite3.Connection | None = None
    deleted_rows = 0
    batches = 0
    try:
        connection = sqlite3.connect(
            checked_path,
            timeout=max(0.0, busy_timeout_ms / 1000.0),
            isolation_level=None,
        )
        connection.execute("PRAGMA busy_timeout = %d" % busy_timeout_ms)
        for _ in range(max_batches):
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "DELETE FROM logs WHERE id IN ("
                    "SELECT id FROM logs WHERE ts < ? ORDER BY ts ASC LIMIT ?"
                    ")",
                    (cutoff_ts, batch_rows),
                )
                deleted = int(connection.execute("SELECT changes()").fetchone()[0] or 0)
                connection.execute("COMMIT")
            except sqlite3.OperationalError as error:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                if is_busy_error(error):
                    after_busy = storage_sizes(checked_path)
                    return {
                        **base,
                        "result": "skipped_busy",
                        "deletedRows": deleted_rows,
                        "batches": batches,
                        "physicalBytesAfter": after_busy["totalBytes"],
                        "physicalBytesDelta": after_busy["totalBytes"] - before["totalBytes"],
                        **after_busy,
                    }
                raise
            deleted_rows += deleted
            batches += 1
            if deleted < batch_rows:
                break
            # The next batch starts a fresh transaction, giving Codex a chance
            # to acquire the write lock between short maintenance transactions.
            time.sleep(0.02)
    except sqlite3.Error as error:
        raise ValidationError("cleanup_failed", "SQLite cleanup could not complete safely") from error
    finally:
        if connection is not None:
            connection.close()

    after = storage_sizes(checked_path)
    result = "cleaned" if deleted_rows else "retention_satisfied"
    return {
        **base,
        "result": result,
        "deletedRows": deleted_rows,
        "batches": batches,
        "physicalBytesAfter": after["totalBytes"],
        "physicalBytesDelta": after["totalBytes"] - before["totalBytes"],
        **after,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Codex SQLite log maintenance helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--path", required=True)
    check_parser.add_argument("--busy-timeout-ms", type=int, default=1000)

    cleanup_parser = subparsers.add_parser("cleanup")
    cleanup_parser.add_argument("--path", required=True)
    cleanup_parser.add_argument("--threshold-mib", type=int, required=True)
    cleanup_parser.add_argument("--retain-hours", type=int, required=True)
    cleanup_parser.add_argument("--batch-rows", type=int, default=1000)
    cleanup_parser.add_argument("--max-batches", type=int, default=5)
    cleanup_parser.add_argument("--busy-timeout-ms", type=int, default=1000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "check":
            payload = check(args.path, bounded_integer(args.busy_timeout_ms, 0, 5000, "busy-timeout-ms"))
        else:
            payload = cleanup(
                args.path,
                args.threshold_mib,
                args.retain_hours,
                args.batch_rows,
                args.max_batches,
                args.busy_timeout_ms,
            )
        emit(payload)
        return 0
    except ValidationError as error:
        emit({"ok": False, "result": "invalid", "errorCode": error.code, "error": error.message})
        return 2
    except Exception as error:  # Defensive boundary: never expose tracebacks to the dashboard.
        emit({"ok": False, "result": "failed", "errorCode": "internal_error", "error": limited_error(error)})
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
