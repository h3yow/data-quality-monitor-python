"""服务门面：组合规则登记、批次登记、执行、影响试算、历史回溯与查询。"""
from __future__ import annotations

from typing import Any

from dqm import backfill, diffing, executor, rules, trial
from dqm.errors import BatchNotFound, DQError
from dqm.models import (
    Batch,
    BatchReport,
    BatchStatus,
    BackfillSummary,
    RuleSpec,
    RuleType,
    RuleVersion,
    Severity,
    TrialReport,
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
        return self._report_from_execution(batch_id, execution)

    @staticmethod
    def _report_from_execution(batch_id: str, execution: Any) -> BatchReport:
        failures = tuple(f for f in execution.findings if f.severity is Severity.FAILURE)
        warnings = tuple(f for f in execution.findings if f.severity is Severity.WARNING)
        status = (BatchStatus.FAILED if failures else BatchStatus.SUCCEEDED)
        return BatchReport(
            batch_id=batch_id,
            status=status,
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

    # ---- 规则升级影响试算（只读）----
    def trial_rule(self, payload: dict[str, Any],
                   batch_ids: list[str] | None = None) -> TrialReport:
        """对一条待定规则做只读影响试算。

        绝不修改生效规则或历史结论；候选规则非法或与现行规则打架时当场拒绝。
        payload 与 register_rule 入参形状一致。
        """
        return trial.trial_rule(self._storage, _parse_spec(payload), batch_ids)

    # ---- 历史批次回溯 ----
    def backfill(self, batch_ids: list[str],
                 interrupts: dict[str, Any] | None = None) -> BackfillSummary:
        """把挑出的历史批次整体按当前生效规则重跑；旧结论保留可对照、可追溯。

        逐批处理：单项失败只标记该项（FAILED），可单独重试，不连累其它批次。
        """
        return backfill.backfill_batches(self._storage, list(batch_ids),
                                         interrupts=interrupts)

    def rerun_one(self, batch_id: str) -> Any:
        """单独重跑（重试）一个历史批次，返回 BackfillItem。"""
        batch = self._storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        summary = backfill.backfill_batches(self._storage, [batch_id])
        return summary.items[0]

    def conclusion_history(self, batch_id: str) -> list[BatchReport]:
        """批次的全部历史结论（旧结论不覆盖；最旧在前，最新为当前结论）。"""
        batch = self._storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        reports: list[BatchReport] = []
        for execution in self._storage.conclusion_lineage(batch_id):
            reports.append(self._report_from_execution(batch_id, execution))
        return reports

    def compare_conclusions(self, batch_id: str) -> Any:
        """并排对照一个批次的最新两版结论（旧 vs 新：翻盘/新增/消失/同点变化）。"""
        batch = self._storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        lineage = self._storage.conclusion_lineage(batch_id)
        if len(lineage) < 2:
            raise DQError(f"批次 {batch_id!r} 只有一版结论，没有可对照的旧结论")
        old_execution, new_execution = lineage[-2], lineage[-1]
        return diffing.impact_for(
            batch_id,
            diffing.status_from_findings(old_execution.findings),
            old_execution.findings, new_execution.findings)
