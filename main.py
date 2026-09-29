"""数据质量校验与监控服务的 HTTP 接口层（薄层，领域逻辑在 dqm 包内）。"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dqm.errors import BatchNotFound, DQError, RuleRejected, ScopeExceeded
from dqm.service import DQService

app = FastAPI(title="Data Quality Monitor")
service = DQService()


def _plain(obj: Any) -> Any:
    """把 dataclass/枚举/元组递归转成可 JSON 化的普通 dict/list/标量。"""
    if is_dataclass(obj):
        data = {k: _plain(v) for k, v in obj.__dict__.items()}
        # 一并带上派生属性（如 flipped_batches、affected_batches）
        for name in dir(type(obj)):
            if name.startswith("_") or name in data:
                continue
            attr = getattr(type(obj), name)
            if isinstance(attr, property):
                data[name] = _plain(getattr(obj, name))
        return data
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (list, tuple)):
        return [_plain(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _plain(v) for k, v in obj.items()}
    return obj


class BatchIn(BaseModel):
    batch_id: str
    rows: list[dict[str, Any]]


class BackfillIn(BaseModel):
    batch_ids: list[str]


class TrialIn(BaseModel):
    rule: dict[str, Any]
    batch_ids: list[str] | None = None


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


@app.post("/rules/trial")
def trial_rule(body: TrialIn) -> dict[str, Any]:
    """只读影响试算：不发布规则、不改历史结论；非法/冲突规则 422，超规模 413。"""
    try:
        report = service.trial_rule(body.rule, body.batch_ids)
    except RuleRejected as exc:
        raise HTTPException(
            status_code=422,
            detail={"reason": exc.reason.value, "detail": exc.detail},
        ) from exc
    except ScopeExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _plain(report)


@app.post("/backfills")
def backfill(body: BackfillIn) -> dict[str, Any]:
    """批量回溯：逐批按当前规则重跑，旧结论留痕；单项失败不连累其它批次。"""
    try:
        summary = service.backfill(body.batch_ids)
    except ScopeExceeded as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _plain(summary)


@app.post("/batches/{batch_id}/backfill")
def backfill_one(batch_id: str) -> dict[str, Any]:
    """单独回溯/重试一个历史批次。"""
    try:
        item = service.rerun_one(batch_id)
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _plain(item)


@app.get("/batches/{batch_id}/history")
def conclusion_history(batch_id: str) -> dict[str, Any]:
    """批次的全部历史结论（旧结论不覆盖；最旧在前，最新为当前结论）。"""
    try:
        reports = service.conclusion_history(batch_id)
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"batch_id": batch_id, "conclusions": [_plain(r) for r in reports]}


@app.get("/batches/{batch_id}/compare")
def compare_conclusions(batch_id: str) -> dict[str, Any]:
    """并排对照最新两版结论：翻盘/新增/消失/同点变化与级别净变化。"""
    try:
        impact = service.compare_conclusions(batch_id)
    except BatchNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except DQError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _plain(impact)
