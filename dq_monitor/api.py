"""FastAPI HTTP 层：请求/响应模型与路由。"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .models import DQError, HTTP_STATUS
from .service import DataQualityService
from .store import Store


# ---------- 请求 / 响应模型 ----------
class FieldIn(BaseModel):
    name: str
    dtype: str
    nullable: bool = True


class SchemaIn(BaseModel):
    name: str
    fields: list[FieldIn]


class RuleIn(BaseModel):
    rule_id: str
    schema_name: str
    field_name: str
    rule_type: str
    severity: str = "ERROR"
    params: dict = Field(default_factory=dict)


class BatchIn(BaseModel):
    batch_id: str
    schema_name: str
    rows: list[dict]


# ---------- 应用工厂 ----------
def create_app(service: DataQualityService | None = None) -> FastAPI:
    app = FastAPI(title="data-quality-monitor", version="0.1.0")
    svc = service or DataQualityService(Store())
    app.state.service = svc

    @app.exception_handler(DQError)
    async def _dq_error_handler(request: Request, exc: DQError) -> JSONResponse:
        # 错误码 -> HTTP 状态，原因可区分、绝不静默
        return JSONResponse(
            status_code=HTTP_STATUS.get(exc.code, 400),
            content={
                "error": {
                    "code": exc.code.value,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/schemas", status_code=201)
    def post_schema(body: SchemaIn) -> dict:
        schema = svc.register_schema(
            body.name, [f.model_dump() for f in body.fields]
        )
        return {
            "name": schema.name,
            "fields": {
                n: {"name": f.name, "dtype": f.dtype, "nullable": f.nullable}
                for n, f in schema.fields.items()
            },
            "created_at": schema.created_at.isoformat(),
        }

    @app.get("/schemas/{name}")
    def get_schema(name: str) -> dict:
        schema = svc.store.get_schema(name)
        if schema is None:
            from .models import ErrorCode

            raise DQError(ErrorCode.SCHEMA_NOT_FOUND, f"schema {name} 不存在",
                          {"schema_name": name})
        return {
            "name": schema.name,
            "fields": {
                n: {"name": f.name, "dtype": f.dtype, "nullable": f.nullable}
                for n, f in schema.fields.items()
            },
        }

    @app.post("/rules", status_code=201)
    def post_rule(body: RuleIn) -> dict:
        rv = svc.register_rule(
            body.rule_id, body.schema_name, body.field_name,
            body.rule_type, body.severity, body.params,
        )
        return {
            "rule_id": rv.rule_id,
            "version": rv.version,
            "schema_name": rv.schema_name,
            "field_name": rv.field_name,
            "rule_type": rv.rule_type.value,
            "severity": rv.severity.value,
            "params": rv.params,
            "status": rv.status.value,
        }

    @app.get("/rules/{rule_id}")
    def get_rule(rule_id: str) -> dict:
        versions = svc.store.get_rule_versions(rule_id)
        if not versions:
            from .models import ErrorCode

            raise DQError(ErrorCode.RULE_NOT_FOUND, f"规则 {rule_id} 不存在",
                          {"rule_id": rule_id})
        return {
            "rule_id": rule_id,
            "latest_version": versions[-1].version,
            "versions": [
                {
                    "version": v.version,
                    "schema_name": v.schema_name,
                    "field_name": v.field_name,
                    "rule_type": v.rule_type.value,
                    "severity": v.severity.value,
                    "params": v.params,
                    "status": v.status.value,
                    "created_at": v.created_at.isoformat(),
                }
                for v in versions
            ],
        }

    @app.post("/batches", status_code=201)
    def post_batch(body: BatchIn) -> dict:
        batch = svc.register_batch(body.batch_id, body.schema_name, body.rows)
        return {
            "batch_id": batch.batch_id,
            "schema_name": batch.schema_name,
            "row_count": len(batch.rows),
            "created_at": batch.created_at.isoformat(),
        }

    @app.post("/batches/{batch_id}/submit")
    def submit_batch(batch_id: str) -> dict:
        execution, accepted = svc.submit(batch_id)
        return {"accepted": accepted, "execution": svc.execution_dict(execution)}

    @app.post("/batches/{batch_id}/retry")
    def retry_batch(batch_id: str) -> dict:
        execution = svc.retry(batch_id)
        return {"accepted": True, "execution": svc.execution_dict(execution)}

    @app.get("/batches/{batch_id}/execution")
    def get_execution(batch_id: str) -> dict:
        return svc.execution_dict(svc.get_execution(batch_id))

    @app.get("/batches/{batch_id}/conclusion")
    def get_conclusion(batch_id: str) -> dict:
        return svc.conclusion(batch_id)

    @app.get("/batches/{batch_id}/findings")
    def get_findings(
        batch_id: str,
        severity: str | None = None,
        rule_id: str | None = None,
    ) -> dict:
        findings = svc.list_findings(batch_id, severity, rule_id)
        return {"batch_id": batch_id, "count": len(findings), "findings": findings}

    @app.post("/batches/{batch_id}/replay")
    def replay_batch(batch_id: str, rule_version: int | None = None) -> dict:
        return svc.replay(batch_id, rule_version)

    @app.get("/stats")
    def get_stats() -> dict:
        s = svc.stats()
        return {
            "schemas": s.schemas,
            "rules": s.rules,
            "rule_versions": s.rule_versions,
            "batches": s.batches,
            "executions": s.executions,
            "errors": s.errors,
            "warnings": s.warnings,
            "infos": s.infos,
        }

    return app


app = create_app()
