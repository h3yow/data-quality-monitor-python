"""服务门面：组合规则登记、批次登记、执行、影响试算与历史回溯查询。"""
from __future__ import annotations

from typing import Any

from dqm import backfill, executor, rules, trial
from dqm.errors import BackfillError, BatchNotFound
from dqm.models import (
    BackfillJob,
    Batch,
    BatchReport,
    BatchStatus,
    Revision,
    RevisionComparison,
    RuleSpec,
    RuleType,
    RuleVersion,
    Severity,
    TrialResult,
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
        latest = self._storage.latest_revision(batch_id)
        return BatchReport(
            batch_id=batch_id,
            status=batch.status,
            execution_id=execution.execution_id,
            rule_versions=dict(execution.rule_versions),
            failures=failures,
            warnings=warnings,
            revision_no=latest.revision_no if latest else None,
        )

    def batch_status(self, batch_id: str) -> BatchStatus:
        batch = self._storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        return batch.status

    # ---- 规则升级影响试算（纯只读）----
    def trial_rule(self, payload: dict[str, Any]) -> TrialResult:
        """对一条待定规则定义做只读影响试算，绝不改动生效规则或历史结论。"""
        return trial.run_trial(self._storage, _parse_spec(payload))

    # ---- 历史批次回溯 ----
    def backfill_batches(self, batch_ids: list[str],
                         max_workers: int | None = None) -> BackfillJob:
        """把一批历史批次整体按当前生效规则回溯重跑（失败隔离、可重试）。"""
        return backfill.backfill_batches(self._storage, list(batch_ids),
                                         max_workers=max_workers)

    def backfill_one(self, batch_id: str) -> RevisionComparison:
        """单独回溯一个批次并返回新旧结论对照。"""
        batch = self._storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        _, reran = backfill.backfill_batch(self._storage, batch)
        return backfill.compare_latest(self._storage, batch_id, reran=reran)

    def retry_backfill_job(self, job_id: str) -> BackfillJob:
        """重试回溯作业中所有未成功的批次。"""
        return backfill.retry_backfill_job(self._storage, job_id)

    def get_backfill_job(self, job_id: str) -> BackfillJob:
        job = self._storage.get_backfill_job(job_id)
        if job is None:
            raise BackfillError(f"回溯作业 {job_id!r} 不存在")
        return job

    def compare_revisions(self, batch_id: str) -> RevisionComparison:
        """并排对照批次最新两个结论版本（旧结论永不覆盖）。"""
        if self._storage.get_batch(batch_id) is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        return backfill.compare_latest(self._storage, batch_id)

    def list_revisions(self, batch_id: str) -> list[Revision]:
        """列出批次全部历史结论版本（含正常执行与回溯产生的版本）。"""
        if self._storage.get_batch(batch_id) is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        return self._storage.revisions(batch_id)
