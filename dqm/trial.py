"""规则升级影响试算（纯只读）：待定规则对历史批次的影响预演。

语义约定：
- 只读：试算只持有 ReadOnlyStorageView，物理上无法触达任何写接口，
  不写规则、不写批次、不写执行，随便重复跑结果一模一样；
- 确定：批次按 batch_id 升序、规则按 rule_id 升序求值，命中顺序由
  executor.evaluate 固定，结论与执行顺序/并发无关；
- 全有或全无：待定规则非法或与现行规则冲突时当场拒绝（TrialRejected），
  不产出半截结果；批次数量超上界时 WorkloadTooLarge，不截断。

试算规则快照：现行生效规则集 + 待定规则。待定 rule_id 与现行规则相同，
视为该规则的"升级候选"，在候选快照中替换现行版本；不同则作为新增规则。
每个批次的"旧结论"取其最近一次已生效修订版本（Revision），因此试算结果
与确认后回溯将要产出的新结论严格对得上。
"""
from __future__ import annotations

import logging

from dqm.errors import RuleRejected, TrialRejected, WorkloadTooLarge
from dqm.executor import MAX_ROWS_PER_BATCH, BatchTooLarge, evaluate, final_status
from dqm.models import (
    BatchImpact,
    BatchStatus,
    Finding,
    RuleSpec,
    RuleVersion,
    Severity,
    TrialResult,
)
from dqm.rules import check_conflicts, validate_spec
from dqm.storage import ReadOnlyStorageView, Storage

logger = logging.getLogger("dqm.trial")

# 处理上界：历史批次可能很多，但不允许无界扫描；超过即明确拒绝。
MAX_TRIAL_BATCHES = 10_000


def _finding_key(f: Finding) -> tuple:
    """命中的身份键：规则/版本/级别/行/字段/判定依据/取值完全一致才算同一条。"""
    return (f.rule_id, f.rule_version, f.severity, f.row_index,
            f.field, f.message, repr(f.value))


def findings_delta(
    old: list[Finding] | tuple[Finding, ...],
    new: list[Finding] | tuple[Finding, ...],
) -> tuple[tuple[Finding, ...], tuple[Finding, ...]]:
    """对比两份命中，返回 (新增, 消失)，保持各自在原列表中的出现顺序。"""
    old_keys = {_finding_key(f) for f in old}
    new_keys = {_finding_key(f) for f in new}
    added = tuple(f for f in new if _finding_key(f) not in old_keys)
    removed = tuple(f for f in old if _finding_key(f) not in new_keys)
    return added, removed


def _counts(findings) -> dict[str, int]:
    return {
        "failures": sum(1 for f in findings if f.severity is Severity.FAILURE),
        "warnings": sum(1 for f in findings if f.severity is Severity.WARNING),
    }


def _severity_breakdown(findings) -> dict[str, int]:
    return {
        Severity.FAILURE.value: sum(
            1 for f in findings if f.severity is Severity.FAILURE),
        Severity.WARNING.value: sum(
            1 for f in findings if f.severity is Severity.WARNING),
    }


def _candidate_snapshot(spec: RuleSpec, view: ReadOnlyStorageView
                        ) -> tuple[RuleVersion, list[RuleVersion]]:
    """校验待定规则并组装"候选规则快照"（不入库）。"""
    try:
        validate_spec(spec)
    except RuleRejected as exc:
        raise TrialRejected(exc.reason, f"待定规则非法，试算拒绝: {exc.detail}") from exc

    current = view.current_rules()
    try:
        # 升级候选与同 id 旧版本是同一条规则的前后版本，不构成"规则间冲突"
        others = [rv for rv in current if rv.spec.rule_id != spec.rule_id]
        check_conflicts(spec, others)
    except RuleRejected as exc:
        raise TrialRejected(exc.reason, f"待定规则与现行规则打架，试算拒绝: {exc.detail}") from exc

    # 候选版本不入库，时间戳无意义；固定哨兵值，保证重复试算结果按值完全一致
    candidate = RuleVersion(
        version=view.current_version(spec.rule_id) + 1, spec=spec, created_at=0.0)
    merged = {rv.spec.rule_id: rv for rv in current}
    merged[spec.rule_id] = candidate
    return candidate, [merged[k] for k in sorted(merged)]


def run_trial(storage: Storage, spec: RuleSpec,
              max_rows: int | None = None) -> TrialResult:
    """用待定规则定义对全部已登记历史批次做只读试算。"""
    if max_rows is None:
        max_rows = MAX_ROWS_PER_BATCH  # 延迟读取，便于按上界常量统一约束
    view = ReadOnlyStorageView(storage)
    candidate, snapshot = _candidate_snapshot(spec, view)
    snapshot_versions = {rv.spec.rule_id: rv.version for rv in snapshot}

    batches = view.list_batches()  # 已按 batch_id 升序
    if len(batches) > MAX_TRIAL_BATCHES:
        raise WorkloadTooLarge(
            f"待试算批次数 {len(batches)} 超过上界 {MAX_TRIAL_BATCHES}，"
            f"请缩小范围后重试（不截断、不给半截结果）")
    oversized = next((b.batch_id for b in batches
                      if len(b.rows) > max_rows), None)
    if oversized is not None:
        raise WorkloadTooLarge(
            f"批次 {oversized!r} 行数超过单批上界 {max_rows}，试算拒绝（不截断）")

    logger.info("影响试算开始: 待定规则=%s type=%s field=%s severity=%s params=%s；"
                "候选快照 rule_versions=%s；历史批次数=%d",
                spec.rule_id, spec.rule_type.value, spec.field,
                spec.severity.value, spec.params, snapshot_versions, len(batches))

    impacts: list[BatchImpact] = []
    added_all: list[Finding] = []
    removed_all: list[Finding] = []
    affected: list[str] = []
    flipped: list[str] = []

    for batch in batches:
        latest = view.latest_revision(batch.batch_id)
        if latest is None:
            # 从未校验的批次：无旧结论，基线为空
            had_conclusion = False
            old_status = BatchStatus.REGISTERED
            old_findings: tuple[Finding, ...] = ()
            old_versions: dict[str, int] = {}
        else:
            had_conclusion = True
            old_status = latest.status
            old_findings = latest.findings
            old_versions = dict(latest.rule_versions)

        try:
            new_findings = evaluate(batch.rows, snapshot)
        except BatchTooLarge as exc:
            # 理论上已被上方预检拦截；此处兜底，仍保证不产出半截结果
            raise WorkloadTooLarge(str(exc)) from exc
        new_status = final_status(new_findings)
        added, removed = findings_delta(old_findings, new_findings)
        is_flipped = had_conclusion and old_status is not new_status

        impacts.append(BatchImpact(
            batch_id=batch.batch_id,
            had_conclusion=had_conclusion,
            old_status=old_status,
            new_status=new_status,
            flipped=is_flipped,
            old_rule_versions=old_versions,
            new_rule_versions=dict(snapshot_versions),
            old_counts=_counts(old_findings),
            new_counts=_counts(new_findings),
            added=added,
            removed=removed,
        ))
        added_all.extend(added)
        removed_all.extend(removed)
        if added or removed:
            affected.append(batch.batch_id)
        if is_flipped:
            flipped.append(batch.batch_id)

        logger.info(
            "试算判定 批次=%s 行数=%d 旧[%s failures=%d warnings=%d] "
            "-> 新[%s failures=%d warnings=%d] 翻盘=%s 新增=%d 消失=%d",
            batch.batch_id, len(batch.rows), old_status.value,
            _counts(old_findings)["failures"], _counts(old_findings)["warnings"],
            new_status.value, _counts(new_findings)["failures"],
            _counts(new_findings)["warnings"], is_flipped, len(added), len(removed))
        for f in added:
            logger.info("  + 新增命中 批次=%s rule=%s v%s severity=%s row=%s 依据=%s",
                        batch.batch_id, f.rule_id, f.rule_version,
                        f.severity.value, f.row_index, f.message)
        for f in removed:
            logger.info("  - 消失命中 批次=%s rule=%s v%s severity=%s row=%s 依据=%s",
                        batch.batch_id, f.rule_id, f.rule_version,
                        f.severity.value, f.row_index, f.message)

    result = TrialResult(
        candidate=candidate,
        rule_versions=snapshot_versions,
        impacts=tuple(impacts),
        affected_batch_ids=tuple(affected),
        flipped_batch_ids=tuple(flipped),
        added_by_severity=_severity_breakdown(added_all),
        removed_by_severity=_severity_breakdown(removed_all),
    )
    logger.info("影响试算完成: 受影响批次=%d %s，翻盘批次=%d %s；"
                "新增命中级别分布=%s，消失命中级别分布=%s",
                len(affected), affected, len(flipped), flipped,
                result.added_by_severity, result.removed_by_severity)
    return result
