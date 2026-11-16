"""FastAPI entrypoint: upload / merge / query API for rain-gauge batches."""
from __future__ import annotations

import contextlib
from typing import List, Optional

from fastapi import FastAPI, File, HTTPException, Path as PathParam, Query, UploadFile
from pydantic import BaseModel, Field

from . import db, services, station_changes
from .config import settings
from .errors import StorageError
from .storage import file_manager


# ----------------------------------------------------------------- schemas
class BatchResponse(BaseModel):
    batch_id: str
    station_id: str
    day: str
    row_count: int
    size_bytes: int
    content_sha256: str
    status: str
    is_merged: bool


class MergeResponse(BaseModel):
    merged_batch_id: str
    station_id: str
    day: str
    source_batch_ids: List[str]
    source_count: int
    row_count: int
    size_bytes: int
    content_sha256: str
    status: str


class RainRecord(BaseModel):
    station_id: str
    obs_time: str
    rain_mm: float
    source_batch_id: str


class QueryResponse(BaseModel):
    station_id: str
    day: str
    snapshot_token: str
    file_count: int
    batch_ids: List[str]
    row_count: int
    truncated: bool
    records: List[RainRecord]


class BatchInfo(BaseModel):
    batch_id: str
    row_count: int
    size_bytes: int
    status: str
    is_merged: bool
    source_batches: Optional[List[str]]
    file_exists: bool
    created_at: str


class ListBatchesResponse(BaseModel):
    station_id: str
    day: str
    batches: List[BatchInfo]


# ------------------------------------------------- station-change schemas
class StationChangeRequest(BaseModel):
    old_station_id: str = Field(..., description="actual station before the switch")
    new_station_id: str = Field(..., description="actual station from the switch on")
    switch_minute: str = Field(
        ...,
        description=(
            "UTC minute at which the new station takes over, e.g. "
            "2026-11-12T12:34Z; the switch minute belongs to the new station"
        ),
    )


class StationChangeUpdate(BaseModel):
    old_station_id: Optional[str] = None
    new_station_id: Optional[str] = None
    switch_minute: Optional[str] = None


class Window(BaseModel):
    from_: str = Field(..., alias="from")
    to_exclusive: str

    model_config = {"populate_by_name": True}


class StationChangeResponse(BaseModel):
    registration_id: str
    logical_station_id: str
    day: str
    old_station_id: str
    new_station_id: str
    switch_minute: str
    old_window: Window
    new_window: Window
    version: int
    created_at: str
    updated_at: str


class ListStationChangesResponse(BaseModel):
    day: Optional[str]
    registrations: List[StationChangeResponse]


class LogicalRecord(BaseModel):
    station_id: str
    logical_station_id: str
    segment: str
    obs_time: str
    rain_mm: float
    source_batch_id: str


class LogicalSegmentInfo(BaseModel):
    station_id: str
    window: Window
    file_count: int
    batch_ids: List[str]


class LogicalQueryResponse(BaseModel):
    logical_station_id: str
    day: str
    registration: StationChangeResponse
    switch_minute: str
    snapshot_token: str
    old_station: LogicalSegmentInfo
    new_station: LogicalSegmentInfo
    file_count: int
    batch_ids: List[str]
    row_count: int
    total_row_count: int
    truncated: bool
    records: List[LogicalRecord]


# ---------------------------------------------------------------- app setup
@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.parquet_root.mkdir(parents=True, exist_ok=True)
    settings.tmp_dir.mkdir(parents=True, exist_ok=True)
    db.init_db()
    file_manager.startup_sweep()
    file_manager.start_reaper()
    yield
    file_manager.stop_reaper()


app = FastAPI(
    title="Rain Gauge Batch Storage API",
    version="1.1.0",
    description=(
        "Upload per-station/per-day rain CSV batches, compact them into one "
        "Parquet file, and query published files with snapshot consistency.\n\n"
        "**Gauge replacement:** register a logical station/day with the old "
        "actual station, the new actual station and the UTC switch minute, then "
        "query `/logical-stations/.../records` to read one continuous series. "
        "The old station is used strictly before the switch minute, the new "
        "station from the switch minute on; duplicate rows, source batch ids "
        "and actual station ids are preserved."
    ),
    lifespan=lifespan,
)


@app.exception_handler(StorageError)
async def storage_error_handler(request, exc: StorageError):
    from fastapi.responses import JSONResponse

    return JSONResponse(
        status_code=exc.status_code,
        content={"error": type(exc).__name__, "detail": str(exc)},
    )


STATION_PATH = PathParam(..., pattern=r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,63}$")
DAY_PATH = PathParam(..., pattern=r"^\d{4}-\d{2}-\d{2}$")


# ----------------------------------------------------------------- endpoints
@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "data_dir": str(settings.data_dir)}


@app.post(
    "/stations/{station_id}/days/{day}/batches",
    response_model=BatchResponse,
    status_code=201,
    tags=["upload"],
    summary="Upload one station/day CSV as an independent batch",
)
async def upload_batch(
    station_id: str = STATION_PATH,
    day: str = DAY_PATH,
    file: UploadFile = File(..., description="CSV with station, UTC time, rain_mm"),
) -> BatchResponse:
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="uploaded file is empty")
    if len(content) > settings.max_upload_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"file exceeds {settings.max_upload_bytes} bytes",
        )
    result = services.upload_csv(station_id, day, content)
    return BatchResponse(**result)


@app.get(
    "/stations/{station_id}/days/{day}/records",
    response_model=QueryResponse,
    tags=["query"],
    summary="Query a station/day by actually reading published Parquet files",
)
def query_records(
    station_id: str = STATION_PATH,
    day: str = DAY_PATH,
    limit: Optional[int] = Query(None, ge=0, description="optional row cap"),
) -> QueryResponse:
    result = services.query_station_day(station_id, day, limit=limit)
    return QueryResponse(**result)


@app.post(
    "/stations/{station_id}/days/{day}/merge",
    response_model=MergeResponse,
    tags=["merge"],
    summary="Compact published small batches into one Parquet file",
)
def merge_day(
    station_id: str = STATION_PATH,
    day: str = DAY_PATH,
) -> MergeResponse:
    result = services.merge_station_day(station_id, day)
    return MergeResponse(**result)


@app.get(
    "/stations/{station_id}/days/{day}/batches",
    response_model=ListBatchesResponse,
    tags=["query"],
    summary="List all batches (published and superseded) for a station/day",
)
def get_batches(
    station_id: str = STATION_PATH,
    day: str = DAY_PATH,
) -> ListBatchesResponse:
    return ListBatchesResponse(**services.list_batches(station_id, day))


@app.post("/admin/reap", tags=["ops"], summary="Force reaping of unreferenced superseded files")
def force_reap() -> dict:
    return services.force_reap()


# ------------------------------------------------- logical station endpoints
LOGICAL_PATH = PathParam(..., pattern=r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,63}$")


@app.post(
    "/logical-stations/{logical_station_id}/days/{day}/changes",
    response_model=StationChangeResponse,
    status_code=201,
    tags=["station-change"],
    summary="Register a gauge replacement (old station, new station, switch minute)",
)
def create_station_change(
    logical_station_id: str = LOGICAL_PATH,
    day: str = DAY_PATH,
    body: StationChangeRequest = ...,
) -> StationChangeResponse:
    return StationChangeResponse(
        **station_changes.register_station_change(
            logical_station_id,
            day,
            body.old_station_id,
            body.new_station_id,
            body.switch_minute,
        )
    )


@app.get(
    "/logical-stations/{logical_station_id}/days/{day}/changes",
    response_model=StationChangeResponse,
    tags=["station-change"],
    summary="Read the registered mapping for one logical station/day",
)
def get_station_change(
    logical_station_id: str = LOGICAL_PATH,
    day: str = DAY_PATH,
) -> StationChangeResponse:
    return StationChangeResponse(
        **station_changes.get_station_change(logical_station_id, day)
    )


@app.put(
    "/logical-stations/{logical_station_id}/days/{day}/changes",
    response_model=StationChangeResponse,
    tags=["station-change"],
    summary="Modify a mapping; a conflicting modification leaves the old one usable",
)
def update_station_change(
    logical_station_id: str = LOGICAL_PATH,
    day: str = DAY_PATH,
    body: StationChangeUpdate = ...,
) -> StationChangeResponse:
    return StationChangeResponse(
        **station_changes.update_station_change(
            logical_station_id,
            day,
            old_station_id=body.old_station_id,
            new_station_id=body.new_station_id,
            switch_minute=body.switch_minute,
        )
    )


@app.get(
    "/station-changes",
    response_model=ListStationChangesResponse,
    tags=["station-change"],
    summary="List registered mappings (optionally filtered by UTC day)",
)
def list_station_changes(
    day: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
) -> ListStationChangesResponse:
    return ListStationChangesResponse(
        **station_changes.list_station_changes(day=day)
    )


@app.get(
    "/logical-stations/{logical_station_id}/days/{day}/records",
    response_model=LogicalQueryResponse,
    tags=["station-change"],
    summary="Query one logical station/day across the gauge replacement",
)
def query_logical_records(
    logical_station_id: str = LOGICAL_PATH,
    day: str = DAY_PATH,
    limit: Optional[int] = Query(None, ge=0, description="optional row cap"),
) -> LogicalQueryResponse:
    return LogicalQueryResponse(
        **station_changes.query_logical_day(
            logical_station_id, day, limit=limit
        )
    )
