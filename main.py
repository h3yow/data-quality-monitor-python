"""数据质量校验与监控服务的 HTTP 接口层（薄层，领域逻辑在 dqm 包内）。"""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dqm.errors import BatchNotFound, RuleRejected
from dqm.service import DQService

app = FastAPI(title="Data Quality Monitor")
service = DQService()


class BatchIn(BaseModel):
    batch_id: str
    rows: list[dict[str, Any]]


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
