"""服务门面：组合规则登记、批次登记、执行与查询。"""
from __future__ import annotations

from typing import Any

from dqm import executor, rules
from dqm.errors import BatchNotFound, DQError
from dqm.models import (
    Batch,
    BatchReport,
    BatchStatus,
    RuleSpec,
    RuleType,
    RuleVersion,
    Severity,
)
from dqm.storage import Storage


def _parse_spec(payload: dict[str, Any]) -> RuleSpec:
    """把外部输入解析为 RuleSpec；缺键/非法值统一抛 RuleRejected。"""
    from dqm.errors import RuleRejected
    from dqm.models import RejectReason

    def require(key: str) -> Any:
        if key not in payload or payload[key] is None:
            raise RuleRejected(RejectReason.MISSING_FIELD, f"缺少必填字段 {key!r}")
        return payload[key]

    try:
        rule_type = RuleType(require("rule_type"))
        severity = Severity(payload.get("severity", Severity.FAILURE.value))
    except ValueError as exc:
        raise RuleRejected(RejectReason.INVALID_DEFINITION, str(exc)) from exc
    return RuleSpec(
        rule_id=require("rule_id"),
        rule_type=rule_type,
        field=require("field"),
        severity=severity,
        params=dict(payload.get("params") or {}),
    )


class DQService:
    def __init__(self, storage: Storage | None = None) -> None:
        self._storage = storage or Storage()

    @property
    def storage(self) -> Storage:
        return self._storage

    def register_rule(self, payload: dict[str, Any]) -> RuleVersion:
        return rules.register_rule(self._storage, _parse_spec(payload))

    def register_batch(self, batch_id: str, rows: list[dict[str, Any]]) -> bool:
        """登记批次；重复 batch_id 返回 False（幂等去重，不覆盖原数据）。"""
        return self._storage.add_batch(Batch(batch_id=batch_id, rows=list(rows)))

    def validate_batch(self, batch_id: str) -> BatchReport:
        """触发（或去重）批次校验，返回稳定结论。

        重复/并发调用只会产生一次有效执行；已完成的批次直接返回既有结论。
        """
        batch = self._storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        if batch.status in (BatchStatus.REGISTERED, BatchStatus.RUNNING):
            executor.run_batch(self._storage, batch)
        return self.get_report(batch_id)

    def get_report(self, batch_id: str) -> BatchReport:
        """查询批次结论：已提交的执行返回其快照结果，未执行的返回空结论。"""
        batch = self._storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        execution = self._storage.committed_execution(batch_id)
        if execution is None or execution.status != "committed":
            return BatchReport(batch_id, batch.status, None, {}, (), ())
        failures = tuple(f for f in execution.findings if f.severity is Severity.FAILURE)
        warnings = tuple(f for f in execution.findings if f.severity is Severity.WARNING)
        return BatchReport(
            batch_id=batch_id,
            status=batch.status,
            execution_id=execution.execution_id,
            rule_versions=dict(execution.rule_versions),
            failures=failures,
            warnings=warnings,
        )

    def batch_status(self, batch_id: str) -> BatchStatus:
        batch = self._storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        return batch.status
