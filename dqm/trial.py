"""规则升级影响试算：只读、确定、可重复；非法/冲突规则当场拒绝。

只读保证：
- 本模块不调用 storage 的任何写方法，不创建执行、不触碰批次状态、
  不改规则版本；候选规则只以临时对象参与内存求值。
- 求值用 executor.evaluate 纯函数，结论只取决于（候选规则 + 批次数据），
  重复运行任意次结果逐字节一致，且与规则登记/批次校验的先后顺序无关。

拒绝保证（不悄悄跳过、不给半截结果）：
- 候选规则非法（validate_spec）或与"假设它发布后"的生效规则集冲突
  （check_conflicts，同 rule_id 的旧版本先剔除）-> 直接抛 RuleRejected；
- 指定批次不存在 -> BatchNotFound；规模超上界 -> ScopeExceeded。
  上述任何一种情况都在求值前拒绝，不返回部分批次的试算结果。
"""
from __future__ import annotations

import logging

from dqm import diffing
from dqm.errors import BatchNotFound, ScopeExceeded
from dqm.executor import evaluate
from dqm.models import BatchImpact, RuleSpec, RuleVersion, TrialReport
from dqm.rules import check_conflicts, validate_spec
from dqm.storage import Storage

# 处理上界：超过即整体拒绝，不截断、不给半截结果
MAX_TRIAL_BATCHES = 500
MAX_TRIAL_ROWS_PER_BATCH = 200_000

logger = logging.getLogger("dqm.trial")


def _hypothetical_snapshot(storage: Storage, spec: RuleSpec) -> tuple[list[RuleVersion], int]:
    """构造"候选规则发布后"的生效规则快照（不落盘）。"""
    candidate_version = storage.current_version(spec.rule_id) + 1
    candidate = RuleVersion(version=candidate_version, spec=spec)
    # 剔除同 rule_id 的旧版本：规则变更时它正是被替换对象，不应判为自冲突
    others = [rv for rv in storage.current_rules() if rv.spec.rule_id != spec.rule_id]
    check_conflicts(spec, others)
    snapshot = sorted(others + [candidate], key=lambda rv: rv.spec.rule_id)
    return snapshot, candidate_version


def trial_rule(storage: Storage, spec: RuleSpec,
               batch_ids: list[str] | None = None) -> TrialReport:
    """拿待定规则对历史批次只读试算；不修改任何生效规则或历史结论。"""
    # 1) 定义合法性与冲突：任一不满足，当场拒绝，不产生任何试算结果
    validate_spec(spec)
    snapshot, candidate_version = _hypothetical_snapshot(storage, spec)
    snapshot_versions = {rv.spec.rule_id: rv.version for rv in snapshot}

    # 2) 圈定试算批次（确定性顺序）
    if batch_ids is None:
        targets = sorted(b.batch_id for b in storage.all_batches())
        explicit = False
    else:
        if len(batch_ids) > MAX_TRIAL_BATCHES:
            raise ScopeExceeded(
                f"试算批次 {len(batch_ids)} 超过上界 {MAX_TRIAL_BATCHES}，"
                "请缩小范围后整体重跑（不截断、不给半截结果）")
        targets = list(dict.fromkeys(batch_ids))  # 去重但保留显式顺序
        explicit = True
    if len(targets) > MAX_TRIAL_BATCHES:
        raise ScopeExceeded(
            f"待试算历史批次 {len(targets)} 超过上界 {MAX_TRIAL_BATCHES}，"
            "请显式指定范围后整体重跑（不截断、不给半截结果）")

    logger.info("试算开始: 候选规则 rule_id=%s v%s type=%s field=%s params=%s, 批次=%s",
                spec.rule_id, candidate_version, spec.rule_type.value, spec.field,
                spec.params, targets)

    impacts: list[BatchImpact] = []
    skipped: list[str] = []
    for batch_id in targets:
        batch = storage.get_batch(batch_id)
        if batch is None:
            if explicit:
                raise BatchNotFound(f"批次 {batch_id!r} 不存在，无法试算")
            continue
        committed = storage.committed_execution(batch_id)
        if committed is None:
            skipped.append(batch_id)  # 尚无生效结论：明确交代为跳过，不混进结果
            continue
        if len(batch.rows) > MAX_TRIAL_ROWS_PER_BATCH:
            raise ScopeExceeded(
                f"批次 {batch_id!r} 行数 {len(batch.rows)} 超过单批上界 "
                f"{MAX_TRIAL_ROWS_PER_BATCH}，请缩小数据范围后整体重跑")

        new_findings = evaluate(batch.rows, snapshot)  # 纯函数，零写入
        impact = diffing.impact_for(
            batch_id,
            diffing.status_from_findings(committed.findings),
            committed.findings, new_findings)
        impacts.append(impact)
        logger.info("试算判定: 批次=%s 输入行数=%d 旧结论=%s 试算结论=%s 翻盘=%s "
                    "净变化(failure/warning)=%s/%s 新增=%d 消失=%d 同点变化=%d",
                    batch_id, len(batch.rows), impact.old_status.value,
                    impact.new_status.value, impact.flipped,
                    impact.delta_by_severity.get("failure", 0),
                    impact.delta_by_severity.get("warning", 0),
                    len(impact.diff.added), len(impact.diff.removed),
                    len(impact.diff.modified))

    report = TrialReport(
        candidate_rule_id=spec.rule_id,
        candidate_version=candidate_version,
        evaluated=tuple(impacts),
        skipped=tuple(skipped),
        rule_versions=snapshot_versions,
    )
    logger.info("试算完成: 候选=%s 已评估=%d 跳过=%d 翻盘批次=%s 受影响批次=%s",
                spec.rule_id, len(impacts), len(skipped),
                report.flipped_batches, report.affected_batches)
    return report
