"""数据质量校验与监控服务的 HTTP 接口层（薄层，领域逻辑在 dqm 包内）。"""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dqm.errors import BackfillError, BatchNotFound, RuleRejected, TrialRejected, WorkloadTooLarge
from dqm.service import DQService

app = FastAPI(title="Data Quality Monitor")
service = DQService()


class BatchIn(BaseModel):
    batch_id: str
    rows: list[dict[str, Any]]


class BackfillIn(BaseModel):
    batch_ids: list[str]
    max_workers: int | None = None


@app.post("/rules", status_code=201)
def register_rule(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        version = service.register_rule(payload)
    except RuleRejected as exc:
        raise HTTPException(
            status_code=422,
            detail={"reason": exc.reason.value, "detail": exc.detail},
        ) from exc
    return {"rule_id": version.spec.rule_id, "version": version.version}


@app.post("/batches", status_code=201)
def register_batch(body: BatchIn) -> dict[str, Any]:
    created = service.register_batch(body.batch_id, body.rows)
    return {"batch_id": body.batch_id, "created": created}


@app.post("/batches/{batch_id}/validate")
def validate_batch(batch_id: str) -> dict[str, Any]:
    try:
        report = service.validate_batch(batch_id)
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except WorkloadTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    return asdict(report)


@app.get("/batches/{batch_id}/report")
def get_report(batch_id: str) -> dict[str, Any]:
    try:
        return asdict(service.get_report(batch_id))
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/batches/{batch_id}/status")
def batch_status(batch_id: str) -> dict[str, str]:
    try:
        return {"batch_id": batch_id, "status": service.batch_status(batch_id).value}
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---- 规则升级影响试算（只读、可重复）----
@app.post("/trials")
def trial_rule(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        result = service.trial_rule(payload)
    except TrialRejected as exc:
        # 待定规则非法或与现行规则打架：当场明确拒绝，不返回半截结果
        raise HTTPException(
            status_code=422,
            detail={"reason": exc.reason.value, "detail": exc.detail},
        ) from exc
    except WorkloadTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    return asdict(result)


# ---- 历史批次回溯 ----
@app.post("/backfills", status_code=201)
def backfill_batches(body: BackfillIn) -> dict[str, Any]:
    try:
        job = service.backfill_batches(body.batch_ids, max_workers=body.max_workers)
    except WorkloadTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except BackfillError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return asdict(job)


@app.get("/backfills/{job_id}")
def get_backfill_job(job_id: str) -> dict[str, Any]:
    try:
        return asdict(service.get_backfill_job(job_id))
    except BackfillError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/backfills/{job_id}/retry")
def retry_backfill_job(job_id: str) -> dict[str, Any]:
    try:
        return asdict(service.retry_backfill_job(job_id))
    except BackfillError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/batches/{batch_id}/backfill")
def backfill_one(batch_id: str) -> dict[str, Any]:
    try:
        return asdict(service.backfill_one(batch_id))
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/batches/{batch_id}/comparison")
def compare_revisions(batch_id: str) -> dict[str, Any]:
    try:
        return asdict(service.compare_revisions(batch_id))
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackfillError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/batches/{batch_id}/revisions")
def list_revisions(batch_id: str) -> dict[str, Any]:
    try:
        revisions = service.list_revisions(batch_id)
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"batch_id": batch_id, "revisions": [asdict(r) for r in revisions]}
