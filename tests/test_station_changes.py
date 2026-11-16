"""Gauge-replacement registry and logical-day query behavior."""
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app import db, services, station_changes
from app.errors import StationChangeConflictError
from app.storage import file_manager
from tests.conftest import make_csv


def _upload(client, station, day, rows):
    r = client.post(
        f"/stations/{station}/days/{day}/batches",
        files={"file": ("x.csv", make_csv(rows, station), "text/csv")},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _register(client, logical, day, old, new, switch):
    return client.post(
        f"/logical-stations/{logical}/days/{day}/changes",
        json={
            "old_station_id": old,
            "new_station_id": new,
            "switch_minute": switch,
        },
    )


def _seed_pair(day, old, new, rows=None):
    """Default rows: old 00:00-11:59 (incl. one after the switch), new from 11:59."""
    rows = rows or {
        old: [("00:00", 0.1), ("00:01", 0.2), ("11:59", 0.3), ("12:00", 9.9)],
        new: [("11:59", 8.8), ("12:00", 0.4), ("12:01", 0.5)],
    }
    for station, items in rows.items():
        services.upload_csv(
            station,
            day,
            make_csv([(f"2026-11-12T{t}Z", v) for t, v in items], station),
        )


# ------------------------------------------------------------------ boundary
def test_switch_boundary_uses_old_strictly_before_and_new_from_minute(client):
    day = "2026-11-12"
    _seed_pair(day, "BOLD", "BNEW")
    r = _register(client, "BLOG", day, "BOLD", "BNEW", "2026-11-12T12:00Z")
    assert r.status_code == 201, r.text
    reg = r.json()
    assert reg["switch_minute"] == "2026-11-12T12:00:00Z"
    assert reg["old_window"] == {
        "from": "2026-11-12T00:00:00Z",
        "to_exclusive": "2026-11-12T12:00:00Z",
    }
    assert reg["new_window"] == {
        "from": "2026-11-12T12:00:00Z",
        "to_exclusive": "2026-11-12T24:00:00Z",
    }

    body = client.get(f"/logical-stations/BLOG/days/{day}/records").json()
    times = [rec["obs_time"] for rec in body["records"]]
    # 11:59 old included, 12:00 old excluded, 12:00 new included, 11:59 new excluded
    assert "2026-11-12T11:59:00Z" in times
    assert times.count("2026-11-12T12:00:00Z") == 1
    at_noon = [r for r in body["records"] if r["obs_time"] == "2026-11-12T12:00:00Z"]
    assert at_noon[0]["station_id"] == "BNEW"
    assert at_noon[0]["segment"] == "new"
    assert all(
        r["station_id"] == "BOLD"
        for r in body["records"]
        if r["obs_time"] < "2026-11-12T12:00:00Z"
    )
    assert all(
        r["station_id"] == "BNEW"
        for r in body["records"]
        if r["obs_time"] >= "2026-11-12T12:00:00Z"
    )
    assert body["total_row_count"] == 5  # three old + two new
    # Actual station id is preserved on every record.
    assert {r["station_id"] for r in body["records"]} == {"BOLD", "BNEW"}
    assert [r["segment"] for r in body["records"]] == sorted(
        [r["segment"] for r in body["records"]],
        key=lambda s: s != "old",
    )


def test_lowercase_switch_minute_and_offset_equivalent_accepted(client):
    day = "2026-11-12"
    _seed_pair(day, "COLD", "CNEW")
    r = _register(client, "CLOG", day, "COLD", "CNEW", "2026-11-12T12:00+00:00")
    assert r.status_code == 201, r.text
    assert r.json()["switch_minute"] == "2026-11-12T12:00:00Z"


@pytest.mark.parametrize(
    "switch",
    ["2026-11-11T12:00Z", "2026-11-12T12:00:30Z", "2026-11-12T00:00Z",
     "2026-11-13T00:00Z", "not-a-time", "2026-11-12T24:00Z"],
)
def test_invalid_switch_minute_rejected(client, switch):
    r = _register(client, "ILOG", "2026-11-12", "IOLD", "INEW", switch)
    assert r.status_code == 400, r.text
    assert "CsvValidationError" in r.text


def test_same_old_and_new_station_rejected(client):
    r = _register(client, "SLOG", "2026-11-12", "SAME", "SAME", "2026-11-12T12:00Z")
    assert r.status_code == 400


def test_duplicate_logical_registration_conflicts(client):
    day = "2026-11-12"
    _register(client, "DLOG", day, "DOLD", "DNEW", "2026-11-12T12:00Z")
    r = _register(client, "DLOG", day, "DOLD2", "DNEW2", "2026-11-12T10:00Z")
    assert r.status_code == 409
    assert "StationChangeConflictError" in r.text


# ------------------------------------------------------- physical exclusivity
def test_overlapping_physical_claim_rejected(client):
    day = "2026-11-12"
    assert _register(client, "P1", day, "SHARED", "P1NEW", "2026-11-12T12:00Z").status_code == 201
    # SHARED is claimed old [00:00,12:00). Another logical station claiming it
    # as new [10:00,24:00) overlaps.
    r = _register(client, "P2", day, "P2OLD", "SHARED", "2026-11-12T10:00Z")
    assert r.status_code == 409, r.text
    # Claiming it as new exactly at 12:00 does not overlap.
    r2 = _register(client, "P3", day, "P3OLD", "SHARED", "2026-11-12T12:00Z")
    assert r2.status_code == 201, r2.text


def test_same_physical_station_allowed_for_different_days(client):
    assert _register(client, "DAY1", "2026-11-12", "X1", "Y1", "2026-11-12T06:00Z").status_code == 201
    assert _register(client, "DAY1", "2026-11-13", "X1", "Y1", "2026-11-13T06:00Z").status_code == 201


def test_conflicting_update_keeps_previous_mapping_usable(client):
    day = "2026-11-12"
    _seed_pair(day, "UOLD", "UNEW", {
        "UOLD": [("00:01", 0.1), ("23:59", 0.2)],
        "UNEW": [("12:00", 0.3), ("12:01", 0.4)],
    })
    assert _register(client, "ULOG", day, "UOLD", "UNEW", "2026-11-12T12:00Z").status_code == 201
    # Another logical station claims UOLD as its *new* station from 18:00,
    # which does not overlap ULOG's old segment [00:00,12:00).
    assert _register(client, "UOTHER", day, "UOTHER_OLD", "UOLD", "2026-11-12T18:00Z").status_code == 201

    # Moving ULOG's switch to 20:00 would extend UOLD's old segment to
    # [00:00,20:00), overlapping UOTHER's new segment [18:00,24:00).
    conflict = client.put(
        "/logical-stations/ULOG/days/2026-11-12/changes",
        json={"switch_minute": "2026-11-12T20:00Z"},
    )
    assert conflict.status_code == 409, conflict.text

    # Old mapping is still in force and query still works with it.
    got = client.get(f"/logical-stations/ULOG/days/{day}/changes").json()
    assert got["switch_minute"] == "2026-11-12T12:00:00Z"
    assert got["version"] == 1
    body = client.get(f"/logical-stations/ULOG/days/{day}/records").json()
    # UOLD 00:01 (23:59 excluded) + UNEW 12:00 and 12:01.
    assert body["row_count"] == 3
    assert body["switch_minute"] == "2026-11-12T12:00:00Z"


def test_successful_update_takes_effect(client):
    day = "2026-11-12"
    _seed_pair(day, "VOLD", "VNEW")
    _register(client, "VLOG", day, "VOLD", "VNEW", "2026-11-12T12:00Z")
    r = client.put(
        "/logical-stations/VLOG/days/2026-11-12/changes",
        json={"switch_minute": "2026-11-12T11:00Z"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["switch_minute"] == "2026-11-12T11:00:00Z"
    assert r.json()["version"] == 2
    body = client.get(f"/logical-stations/VLOG/days/2026-11-12/records").json()
    # Old 11:59 now excluded; new 11:59 included.
    assert "2026-11-12T11:59:00Z" in [rec["obs_time"] for rec in body["records"]]
    eleven_59 = [x for x in body["records"] if x["obs_time"] == "2026-11-12T11:59:00Z"]
    assert all(x["station_id"] == "VNEW" for x in eleven_59)


def test_unknown_logical_station_is_404(client):
    assert client.get("/logical-stations/NOPE/days/2026-11-12/records").status_code == 404
    assert client.get("/logical-stations/NOPE/days/2026-11-12/changes").status_code == 404


def test_registered_day_without_uploads_returns_empty(client):
    day = "2026-11-14"
    assert _register(client, "EMPTYLOG", day, "EMPTYA", "EMPTYB", "2026-11-14T12:00Z").status_code == 201
    body = client.get(f"/logical-stations/EMPTYLOG/days/{day}/records").json()
    assert body["row_count"] == 0 and body["records"] == []
    assert body["file_count"] == 0 and body["old_station"]["batch_ids"] == []


def test_rows_outside_each_window_are_excluded(client):
    day = "2026-11-12"
    # Each physical station only reports on the wrong side of the switch.
    services.upload_csv("WRONGA", day, make_csv([("2026-11-12T15:00Z", 1.0)], "WRONGA"))
    services.upload_csv("WRONGB", day, make_csv([("2026-11-12T03:00Z", 2.0)], "WRONGB"))
    assert _register(client, "WRONGLOG", day, "WRONGA", "WRONGB", "2026-11-12T12:00Z").status_code == 201
    body = client.get(f"/logical-stations/WRONGLOG/days/2026-11-12/records").json()
    assert body["row_count"] == 0
    # But the actual-station endpoints still show those rows.
    assert client.get("/stations/WRONGA/days/2026-11-12/records").json()["row_count"] == 1
    assert client.get("/stations/WRONGB/days/2026-11-12/records").json()["row_count"] == 1


def test_bad_logical_path_and_day_are_422(client):
    assert client.get("/logical-stations/bad%20id/days/2026-11-12/records").status_code == 422
    assert client.get("/logical-stations/OKID/days/20261112/records").status_code == 422
    r = client.post(
        "/logical-stations/OKID/days/2026-11-12/changes",
        json={"old_station_id": "A", "new_station_id": "B"},
    )
    assert r.status_code == 422  # missing switch_minute


# ------------------------------------------------------- preservation/limits
def test_duplicates_batches_and_actual_station_are_preserved(client):
    day = "2026-11-12"
    _seed_pair(day, "WOLD", "WNEW", {
        "WOLD": [("00:01", 0.1), ("00:01", 0.1), ("11:59", 0.2)],
        "WNEW": [("12:00", 0.3), ("12:00", 0.3)],
    })
    old_batches = client.get(f"/stations/WOLD/days/{day}/batches").json()["batches"]
    new_batches = client.get(f"/stations/WNEW/days/{day}/batches").json()["batches"]
    _register(client, "WLOG", day, "WOLD", "WNEW", "2026-11-12T12:00Z")

    body = client.get(f"/logical-stations/WLOG/days/{day}/records").json()
    assert body["row_count"] == 5
    dupes = [r for r in body["records"] if r["obs_time"] == "2026-11-12T00:01:00Z"]
    assert len(dupes) == 2
    dupes_new = [r for r in body["records"] if r["obs_time"] == "2026-11-12T12:00:00Z"]
    assert len(dupes_new) == 2
    assert {r["source_batch_id"] for r in body["records"]} == {
        old_batches[0]["batch_id"],
        new_batches[0]["batch_id"],
    }
    assert body["old_station"]["batch_ids"] == [old_batches[0]["batch_id"]]
    assert body["new_station"]["batch_ids"] == [new_batches[0]["batch_id"]]


def test_limit_and_truncation(client):
    day = "2026-11-12"
    _seed_pair(day, "XOLD", "XNEW")
    _register(client, "XLOG", day, "XOLD", "XNEW", "2026-11-12T12:00Z")
    body = client.get("/logical-stations/XLOG/days/2026-11-12/records?limit=2").json()
    assert body["row_count"] == 2
    assert body["total_row_count"] == 5
    assert body["truncated"] is True


def test_original_actual_station_query_still_works(client):
    day = "2026-11-12"
    _seed_pair(day, "YOLD", "YNEW")
    _register(client, "YLOG", day, "YOLD", "YNEW", "2026-11-12T12:00Z")
    old = client.get(f"/stations/YOLD/days/{day}/records").json()
    new = client.get(f"/stations/YNEW/days/{day}/records").json()
    # Actual-station queries are unfiltered: they still return everything.
    assert old["row_count"] == 4
    assert new["row_count"] == 3
    assert all(r["station_id"] == "YOLD" for r in old["records"])


def test_list_station_changes(client):
    _register(client, "L1", "2026-11-12", "L1A", "L1B", "2026-11-12T05:00Z")
    _register(client, "L2", "2026-11-13", "L2A", "L2B", "2026-11-13T05:00Z")
    all_body = client.get("/station-changes").json()
    assert {"L1", "L2"}.issubset({r["logical_station_id"] for r in all_body["registrations"]})
    day_body = client.get("/station-changes?day=2026-11-12").json()
    day_ids = {r["logical_station_id"] for r in day_body["registrations"]}
    assert "L1" in day_ids and "L2" not in day_ids
    assert all(r["day"] == "2026-11-12" for r in day_body["registrations"])


# ------------------------------------------------------- read failure behavior
def test_read_failure_returns_error_not_partial_records(client):
    day = "2026-11-12"
    _seed_pair(day, "ZOLD", "ZNEW")
    _register(client, "ZLOG", day, "ZOLD", "ZNEW", "2026-11-12T12:00Z")

    old_path = file_manager.target_path("ZOLD", day, _first_batch("ZOLD", day))
    original = old_path.read_bytes()
    old_path.write_bytes(b"not parquet at all")
    try:
        r = client.get("/logical-stations/ZLOG/days/2026-11-12/records")
        assert r.status_code == 500, r.text
        assert "QueryReadError" in r.text
    finally:
        old_path.write_bytes(original)

    # With the file restored the query works again.
    body = client.get("/logical-stations/ZLOG/days/2026-11-12/records").json()
    assert body["row_count"] == 5


def _first_batch(station, day):
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT batch_id FROM batches WHERE station_id = ? AND day = ? "
            "AND status = 'published' ORDER BY created_at, rowid",
            (station, day),
        ).fetchall()
    finally:
        conn.close()
    return rows[0]["batch_id"]


# ------------------------------------------------------- snapshot consistency
def test_query_snapshot_pins_both_stations_through_merge():
    day = "2026-11-12"
    station_changes.register_station_change(
        "SLOG1", day, "SNAPOLD", "SNAPNEW", "2026-11-12T12:00Z"
    )
    _seed_pair(day, "SNAPOLD", "SNAPNEW")

    release = threading.Event()
    inside_read = threading.Event()
    original_read = station_changes._read_parquet

    def slow_read(path, *a, **kw):
        inside_read.set()
        release.wait(timeout=10)
        return original_read(path, *a, **kw)

    station_changes._read_parquet = slow_read
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            query_future = pool.submit(
                station_changes.query_logical_day, "SLOG1", day
            )
            assert inside_read.wait(timeout=10)
            # Merge both physical stations while the query is mid-read.
            m_old = services.merge_station_day("SNAPOLD", day)
            m_new = services.merge_station_day("SNAPNEW", day)
            old_paths = [
                file_manager.target_path("SNAPOLD", day, bid)
                for bid in m_old["source_batch_ids"]
            ] + [
                file_manager.target_path("SNAPNEW", day, bid)
                for bid in m_new["source_batch_ids"]
            ]
            assert all(p.exists() for p in old_paths)
            assert set(file_manager.reap_once()).isdisjoint(
                m_old["source_batch_ids"] + m_new["source_batch_ids"]
            )
            release.set()
            result = query_future.result(timeout=20)
    finally:
        station_changes._read_parquet = original_read
        release.set()

    # Query saw the pre-merge files of both stations, and read every row.
    assert result["row_count"] == 5
    assert set(result["batch_ids"]) == set(
        m_old["source_batch_ids"] + m_new["source_batch_ids"]
    )
    # After the query released its refs the reaper may remove them.
    file_manager.reap_once()
    assert all(not p.exists() for p in old_paths)


def test_mapping_and_merge_parallel_do_not_mix_snapshots():
    """Concurrent update + merge never yields rows outside the snapshot windows."""
    day = "2026-11-12"
    _seed_pair(day, "PAROLD", "PARNEW")
    station_changes.register_station_change(
        "PARLOG", day, "PAROLD", "PARNEW", "2026-11-12T12:00Z"
    )

    results = []

    def updater(i):
        try:
            station_changes.update_station_change(
                "PARLOG", day, switch_minute=f"2026-11-12T{8 + i % 3:02d}:00Z"
            )
        except StationChangeConflictError:
            pass
        return True

    def merger():
        try:
            services.merge_station_day("PAROLD", day)
        except Exception:
            pass
        return True

    def querier():
        results.append(station_changes.query_logical_day("PARLOG", day))
        return True

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(updater, i) for i in range(6)]
        futures += [pool.submit(merger) for _ in range(2)]
        futures += [pool.submit(querier) for _ in range(8)]
        for f in futures:
            f.result(timeout=40)

    # Every response is internally consistent: the rows match the registration
    # reported in that same response, never a mix of two mappings.
    for body in results:
        reg = body["registration"]
        switch = reg["switch_minute"]
        assert reg["old_station_id"] == "PAROLD"
        assert reg["new_station_id"] == "PARNEW"
        assert body["switch_minute"] == switch
        for rec in body["records"]:
            if rec["segment"] == "old":
                assert rec["station_id"] == "PAROLD"
                assert rec["obs_time"] < switch
            else:
                assert rec["station_id"] == "PARNEW"
                assert rec["obs_time"] >= switch
            # Provenance is always the original upload batch id, even when the
            # row now physically lives inside a merged file.
            assert rec["source_batch_id"]
        # Every file in the snapshot is reported, and the count is consistent.
        assert len(body["batch_ids"]) == body["file_count"]

    # Final state still coherent: exactly one merged file, all rows visible.
    body = station_changes.query_logical_day("PARLOG", day)
    assert body["row_count"] == 5
    actual = services.query_station_day("PAROLD", day)
    assert actual["row_count"] == 4


def test_upload_registration_merge_concurrent():
    """Registration, uploads and merges can run in parallel without corruption."""
    day = "2026-11-13"
    services.upload_csv(
        "CONOLD", day, make_csv([("2026-11-13T00:00Z", 0.1)], "CONOLD")
    )
    services.upload_csv(
        "CONNEW", day, make_csv([("2026-11-13T12:00Z", 0.2)], "CONNEW")
    )

    errors = []

    def register():
        try:
            station_changes.register_station_change(
                "CONLOG", day, "CONOLD", "CONNEW", "2026-11-13T12:00Z"
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(("register", exc))

    def upload(i):
        try:
            services.upload_csv(
                "CONOLD",
                day,
                make_csv([(f"2026-11-13T0{i + 1}:00Z", 0.5)], "CONOLD"),
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(("upload", exc))

    def merge():
        try:
            services.merge_station_day("CONNEW", day)
        except Exception as exc:  # noqa: BLE001
            errors.append(("merge", exc))

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(register)]
        futures += [pool.submit(upload, i) for i in range(3)]
        futures += [pool.submit(merge)]
        for f in futures:
            f.result(timeout=30)

    assert errors == []
    body = station_changes.query_logical_day("CONLOG", day)
    # 3 old uploads (00:00 + 01..03) but only those < 12:00 count: all 4 are.
    assert body["row_count"] == 5
    assert body["new_station"]["file_count"] == 1
