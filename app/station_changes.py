"""Gauge-replacement (station-change) registration and logical-day queries.

A rain gauge can be replaced mid-day: the old physical station reports until
some UTC minute, the new physical station reports from that minute onward
(the switch minute belongs to the **new** station).  A *logical station* maps
one UTC day onto exactly one such ``(old_station, new_station, switch)`` tuple.

Consistency rules implemented here:

* **One registration per logical station and day** (unique index in
  :mod:`app.db`).
* **No physical station may be claimed by two logical stations for the same
  day and an overlapping part of its timeline.**  The old segment covers
  ``[00:00, switch)`` and the new segment ``[switch, 24:00)``; overlap is
  decided on those half-open minute intervals.
* ``old_station_id != new_station_id`` and the switch must be a whole UTC
  minute strictly inside the requested day.
* A conflicting *create* is rejected.  A conflicting *update* is rejected too
  and, because the update transaction rolls back, the previous mapping remains
  fully usable.

Snapshot consistency for :func:`query_logical_day`:

The mapping row and the published-file sets of **both** physical stations are
selected while holding the in-process writer gate (:data:`app.db.write_lock`).
Upload, merge and registration writes all take that same gate, so the mapping
and the file sets come from one and the same serialization point.  The file
sets are pinned with the file manager's reference counter before the lock is
released, so a merge finishing afterwards cannot unlink them mid-query
(``reap_once`` skips pinned files).

Reading is all-or-nothing: every file of both stations is read before any
record is returned, and any read failure aborts the whole query with an error
instead of returning a partial result.
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

from . import db
from .errors import (
    CsvValidationError,
    NotFoundError,
    QueryReadError,
    StationChangeConflictError,
)
from .ingestion import validate_day, validate_station_id
from .storage import FileManager, FileRef, file_manager

# Canonical minute representation stored in SQLite.
MINUTE_FORMAT = "%Y-%m-%dT%H:%M:00Z"

_MINUTES_PER_DAY = 24 * 60


# ------------------------------------------------------------------ helpers
def _new_registration_id() -> str:
    return f"SC-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:10]}"


def _day_start(day: str) -> datetime:
    return datetime.combine(date.fromisoformat(day), time.min, tzinfo=timezone.utc)


def parse_switch_minute(switch_minute: str, day: str) -> datetime:
    """Validate ``switch_minute`` and return it as an aware UTC datetime.

    Accepted shapes: ``YYYY-MM-DDTHH:MMZ``, ``YYYY-MM-DDTHH:MM:00Z`` and the
    equivalent ``+00:00`` ISO forms.  Seconds, when present, must be zero.
    """
    if not isinstance(switch_minute, str):
        raise CsvValidationError("switch_minute must be an ISO-8601 UTC minute string")
    raw = switch_minute.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise CsvValidationError(
            f"invalid switch_minute {switch_minute!r}: expected YYYY-MM-DDTHH:MMZ"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    if dt.second or dt.microsecond:
        raise CsvValidationError(
            f"switch_minute {switch_minute!r} must be an exact UTC minute"
        )
    if dt.date().isoformat() != day:
        raise CsvValidationError(
            f"switch_minute {switch_minute!r} is not within UTC day {day}"
        )
    return dt


def _canonical_minute(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(MINUTE_FORMAT)


def _minute_of_day(canonical_minute: str) -> int:
    dt = datetime.strptime(canonical_minute, MINUTE_FORMAT)
    return dt.hour * 60 + dt.minute


def _row_to_registration(row: sqlite3.Row) -> dict:
    return {
        "registration_id": row["registration_id"],
        "logical_station_id": row["logical_station_id"],
        "day": row["day"],
        "old_station_id": row["old_station_id"],
        "new_station_id": row["new_station_id"],
        "switch_minute": row["switch_minute"],
        "old_window": {
            "from": f"{row['day']}T00:00:00Z",
            "to_exclusive": row["switch_minute"],
        },
        "new_window": {
            "from": row["switch_minute"],
            "to_exclusive": f"{row['day']}T24:00:00Z",
        },
        "version": row["version"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def validate_registration(
    logical_station_id: str,
    day: str,
    old_station_id: str,
    new_station_id: str,
    switch_minute: str,
) -> tuple[datetime, str]:
    """Validate inputs and return ``(switch_datetime, canonical_switch)``."""
    validate_station_id(logical_station_id)
    validate_day(day)
    validate_station_id(old_station_id)
    validate_station_id(new_station_id)
    if old_station_id == new_station_id:
        raise CsvValidationError("old_station_id and new_station_id must differ")
    dt = parse_switch_minute(switch_minute, day)
    start = _day_start(day)
    if dt <= start or dt >= start + timedelta(days=1):
        raise CsvValidationError(
            "switch_minute must be strictly inside the UTC day (00:00 < t < 24:00)"
        )
    return dt, _canonical_minute(dt)


# ------------------------------------------------------------ conflict check
def _physical_claims(
    conn: sqlite3.Connection,
    day: str,
    exclude_registration_id: str | None = None,
) -> list[dict]:
    """All physical-station claims for ``day`` as half-open minute intervals."""
    rows = conn.execute(
        "SELECT registration_id, logical_station_id, old_station_id, "
        "new_station_id, switch_minute FROM station_changes WHERE day = ?",
        (day,),
    ).fetchall()
    claims: list[dict] = []
    for row in rows:
        if exclude_registration_id and row["registration_id"] == exclude_registration_id:
            continue
        switch = _minute_of_day(row["switch_minute"])
        claims.append(
            {
                "logical_station_id": row["logical_station_id"],
                "station_id": row["old_station_id"],
                "role": "old",
                "start": 0,
                "end": switch,
            }
        )
        claims.append(
            {
                "logical_station_id": row["logical_station_id"],
                "station_id": row["new_station_id"],
                "role": "new",
                "start": switch,
                "end": _MINUTES_PER_DAY,
            }
        )
    return claims


def _check_no_physical_overlap(
    conn: sqlite3.Connection,
    *,
    day: str,
    old_station_id: str,
    new_station_id: str,
    switch_dt: datetime,
    exclude_registration_id: str | None = None,
) -> None:
    """Reject if a physical station is claimed twice for overlapping minutes.

    A single registration legitimately uses two different physical stations,
    so only rows of *other* registrations are compared.
    """
    switch = switch_dt.hour * 60 + switch_dt.minute
    wanted = [
        (old_station_id, 0, switch),
        (new_station_id, switch, _MINUTES_PER_DAY),
    ]
    for claim in _physical_claims(conn, day, exclude_registration_id):
        for station_id, start, end in wanted:
            if claim["station_id"] != station_id:
                continue
            if start < claim["end"] and claim["start"] < end:
                raise StationChangeConflictError(
                    f"physical station {station_id} is already registered for an "
                    f"overlapping part of {day} by logical station "
                    f"{claim['logical_station_id']} ({claim['role']} segment)"
                )


# --------------------------------------------------------------- create/read
def register_station_change(
    logical_station_id: str,
    day: str,
    old_station_id: str,
    new_station_id: str,
    switch_minute: str,
) -> dict:
    """Create the mapping for one logical station/day."""
    switch_dt, canonical_switch = validate_registration(
        logical_station_id, day, old_station_id, new_station_id, switch_minute
    )
    registration_id = _new_registration_id()
    with db.transaction() as conn:
        existing = conn.execute(
            "SELECT registration_id FROM station_changes "
            "WHERE logical_station_id = ? AND day = ?",
            (logical_station_id, day),
        ).fetchone()
        if existing is not None:
            raise StationChangeConflictError(
                f"logical station {logical_station_id} already has a registration "
                f"for {day}; use PUT to replace it"
            )
        _check_no_physical_overlap(
            conn,
            day=day,
            old_station_id=old_station_id,
            new_station_id=new_station_id,
            switch_dt=switch_dt,
        )
        try:
            conn.execute(
                "INSERT INTO station_changes "
                "(registration_id, logical_station_id, day, old_station_id, "
                " new_station_id, switch_minute) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    registration_id,
                    logical_station_id,
                    day,
                    old_station_id,
                    new_station_id,
                    canonical_switch,
                ),
            )
        except sqlite3.IntegrityError as exc:  # pragma: no cover - race only
            raise StationChangeConflictError(
                f"logical station {logical_station_id} already has a registration "
                f"for {day}"
            ) from exc
        row = conn.execute(
            "SELECT * FROM station_changes WHERE registration_id = ?",
            (registration_id,),
        ).fetchone()
    return _row_to_registration(row)


def get_station_change(logical_station_id: str, day: str) -> dict:
    validate_station_id(logical_station_id)
    validate_day(day)
    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT * FROM station_changes WHERE logical_station_id = ? AND day = ?",
            (logical_station_id, day),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise NotFoundError(
            f"no station change registered for {logical_station_id} / {day}"
        )
    return _row_to_registration(row)


def list_station_changes(day: str | None = None) -> dict:
    if day is not None:
        validate_day(day)
    conn = db.connect()
    try:
        if day is None:
            rows = conn.execute(
                "SELECT * FROM station_changes ORDER BY day ASC, logical_station_id ASC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM station_changes WHERE day = ? "
                "ORDER BY logical_station_id ASC",
                (day,),
            ).fetchall()
    finally:
        conn.close()
    return {"day": day, "registrations": [_row_to_registration(r) for r in rows]}


def update_station_change(
    logical_station_id: str,
    day: str,
    *,
    old_station_id: str | None = None,
    new_station_id: str | None = None,
    switch_minute: str | None = None,
) -> dict:
    """Partially update a registration.

    All values are validated and conflict-checked inside one serialized
    transaction.  On any error the transaction rolls back, leaving the
    previous mapping untouched and still usable by queries.
    """
    validate_station_id(logical_station_id)
    validate_day(day)
    with db.transaction() as conn:
        current = conn.execute(
            "SELECT * FROM station_changes WHERE logical_station_id = ? AND day = ?",
            (logical_station_id, day),
        ).fetchone()
        if current is None:
            raise NotFoundError(
                f"no station change registered for {logical_station_id} / {day}"
            )
        resolved_old = (
            old_station_id if old_station_id is not None else current["old_station_id"]
        )
        resolved_new = (
            new_station_id if new_station_id is not None else current["new_station_id"]
        )
        requested_switch = (
            switch_minute if switch_minute is not None else current["switch_minute"]
        )
        switch_dt, canonical_switch = validate_registration(
            logical_station_id, day, resolved_old, resolved_new, requested_switch
        )
        _check_no_physical_overlap(
            conn,
            day=day,
            old_station_id=resolved_old,
            new_station_id=resolved_new,
            switch_dt=switch_dt,
            exclude_registration_id=current["registration_id"],
        )
        conn.execute(
            "UPDATE station_changes SET old_station_id = ?, new_station_id = ?, "
            "switch_minute = ?, version = version + 1, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
            "WHERE registration_id = ?",
            (resolved_old, resolved_new, canonical_switch, current["registration_id"]),
        )
        row = conn.execute(
            "SELECT * FROM station_changes WHERE registration_id = ?",
            (current["registration_id"],),
        ).fetchone()
    return _row_to_registration(row)


# ------------------------------------------------------------------- reading
def _refs_from_rows(rows: Iterable[sqlite3.Row]) -> list[FileRef]:
    return [
        FileRef(
            batch_id=r["batch_id"],
            path=Path(r["file_path"]),
            row_count=r["row_count"],
            is_merged=bool(r["is_merged"]),
            created_at=r["created_at"],
        )
        for r in rows
    ]


def _read_parquet(path: Path):
    """Indirection so tests can inject read failures/delays per query."""
    return pq.read_table(path)


def _read_window(
    refs: list[FileRef],
    day: str,
    start_min: int,
    end_min: int,
    segment: str,
    logical_station_id: str,
) -> list[dict]:
    """Read all refs and keep records in the half-open minute window.

    Every file is read before returning; a read error propagates as
    :class:`QueryReadError`, so no partial result can ever be returned.
    Row order and duplicate rows are preserved exactly.
    """
    day_start = _day_start(day)
    records: list[dict] = []
    for ref in refs:
        try:
            table = _read_parquet(ref.path)
        except Exception as exc:
            raise QueryReadError(
                f"failed to read published batch {ref.batch_id} "
                f"({ref.path.name}): {exc}"
            ) from exc
        cols = {name: table.column(name).to_pylist() for name in table.column_names}
        for i in range(table.num_rows):
            dt: datetime = cols["obs_time"][i].astimezone(timezone.utc)
            minute = int((dt - day_start).total_seconds() // 60)
            if start_min <= minute < end_min:
                records.append(
                    {
                        "station_id": cols["station_id"][i],
                        "logical_station_id": logical_station_id,
                        "segment": segment,
                        "obs_time": dt.isoformat().replace("+00:00", "Z"),
                        "rain_mm": cols["rain_mm"][i],
                        "source_batch_id": cols["source_batch_id"][i],
                    }
                )
    return records


def _load_batch_rows(
    conn: sqlite3.Connection, station_id: str, day: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT batch_id, file_path, row_count, is_merged, created_at "
        "FROM batches WHERE station_id = ? AND day = ? AND status = 'published' "
        "ORDER BY created_at ASC, rowid ASC",
        (station_id, day),
    ).fetchall()


def query_logical_day(
    logical_station_id: str,
    day: str,
    limit: int | None = None,
    fm: FileManager = file_manager,
) -> dict:
    """Query a logical station's UTC day across a gauge replacement.

    Old-station records strictly before the switch minute and new-station
    records at/after it; every other record of those two physical stations is
    excluded.  Duplicate rows, source batch ids and the actual station id are
    preserved.  The mapping and both pinned file sets come from one snapshot.
    """
    validate_station_id(logical_station_id)
    validate_day(day)

    # ---- atomic selection: mapping + both published file sets, pinned ----
    # db.write_lock is taken by every upload / merge / registration write, so
    # the SELECTs below observe one single committed state.  Pinning happens
    # inside the gate and before it is released, so the reaper cannot remove
    # those files once a merge supersedes them.
    with db.write_lock:
        conn = db.connect()
        try:
            row = conn.execute(
                "SELECT * FROM station_changes "
                "WHERE logical_station_id = ? AND day = ?",
                (logical_station_id, day),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"no station change registered for {logical_station_id} / {day}"
                )
            registration = _row_to_registration(row)
            old_refs = _refs_from_rows(
                _load_batch_rows(conn, row["old_station_id"], day)
            )
            new_refs = _refs_from_rows(
                _load_batch_rows(conn, row["new_station_id"], day)
            )
        finally:
            conn.close()

        fm.pin_refs(old_refs + new_refs)

    try:
        switch_min = _minute_of_day(row["switch_minute"])
        old_records = _read_window(
            old_refs, day, 0, switch_min, "old", logical_station_id
        )
        new_records = _read_window(
            new_refs,
            day,
            switch_min,
            _MINUTES_PER_DAY,
            "new",
            logical_station_id,
        )
        records = old_records + new_records
    finally:
        # Released only after every file has been read (or the read failed).
        fm.unpin_refs(old_refs + new_refs)

    # Stable sort by observation time; duplicates and per-segment order stay.
    records.sort(key=lambda r: (r["obs_time"], r["segment"] != "old"))

    total_rows = len(records)
    truncated = False
    if limit is not None and limit >= 0 and total_rows > limit:
        records = records[:limit]
        truncated = True

    return {
        "logical_station_id": logical_station_id,
        "day": day,
        "registration": registration,
        "switch_minute": row["switch_minute"],
        "snapshot_token": (
            f"L:{logical_station_id}/{day}@"
            f"{datetime.now(timezone.utc).isoformat()}"
        ),
        "old_station": {
            "station_id": row["old_station_id"],
            "window": registration["old_window"],
            "file_count": len(old_refs),
            "batch_ids": [r.batch_id for r in old_refs],
        },
        "new_station": {
            "station_id": row["new_station_id"],
            "window": registration["new_window"],
            "file_count": len(new_refs),
            "batch_ids": [r.batch_id for r in new_refs],
        },
        "file_count": len(old_refs) + len(new_refs),
        "batch_ids": [r.batch_id for r in old_refs]
        + [r.batch_id for r in new_refs],
        "row_count": len(records),
        "total_row_count": total_rows,
        "truncated": truncated,
        "records": records,
    }
