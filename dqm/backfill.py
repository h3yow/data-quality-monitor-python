"""历史批次回溯：按当前生效规则重跑，旧结论留痕；并发去重、单项可重试。

并发与幂等：
- 重跑入口走 storage.try_begin_rerun 的 compare-and-set；同一批次无论被
  重复/并发地从回溯还是正常校验丢进来，同一时刻只有一次真正执行；
- 冲突方在跑时等待其落定并复用同一份结果（不重复执行、计数不翻倍），
  冲突方中止则重新抢权重试；
- 当前生效规则快照与上一版结论一致时直接跳过（CURRENT），重复回溯是幂等的。

留痕与隔离：
- 提交只追加结论谱系，旧结论不覆盖、不抹除，可随时查询对照；
- 批量回溯逐批串行处理：单个批次失败/中断只把该项标记 FAILED 并记下原因，
  不连累其它批次，也不留下半完成结果；FAILED 项可单独重试到成功。
- 串行 + 按 batch_id 排序使结论与"逐个处理"完全一致，与回溯先后顺序无关。
"""
from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

from dqm import diffing
from dqm.errors import BatchNotFound, BatchNotValidated, ScopeExceeded
from dqm.executor import _final_status, _wait_settled, evaluate
from dqm.models import (
    BackfillItem,
    BackfillState,
    BackfillSummary,
    Batch,
    BatchStatus,
    Execution,
)
from dqm.storage import Storage

# 处理上界：超过即整体拒绝；逐批串行处理，结论与逐个处理完全一致
MAX_BACKFILL_BATCHES = 500
MAX_BACKFILL_ROWS_PER_BATCH = 2_000_000

logger = logging.getLogger("dqm.backfill")


def _item_skipped(batch_id: str, current: Execution) -> BackfillItem:
    status = _final_status(current.findings)
    return BackfillItem(
        batch_id=batch_id, state=BackfillState.CURRENT,
        old_status=status, new_status=status, flipped=False,
        diff=diffing.diff_findings(current.findings, current.findings),
        new_execution_id=current.execution_id,
        delta_by_severity={"failure": 0, "warning": 0})


def _rerun_once(storage: Storage, batch: Batch,
                interrupt: Callable[[], None] | None = None
                ) -> tuple[Execution, Execution | None, str]:
    """尝试一次重跑。状态码：

    - "ran"：本调用抢到执行权并提交，返回 (新执行, 上一版执行, "ran")；
    - "current"：最新结论已按本次快照定论（并发方抢先跑完），返回
      (最新执行, 最新执行, "current")，调用方幂等跳过；
    - "inflight"：另有执行在跑，返回 (进行中执行, None, "inflight")，
      调用方等待其落定。
    """
    previous = storage.committed_execution(batch.batch_id)
    if previous is None:
        raise BatchNotValidated(
            f"批次 {batch.batch_id!r} 尚无生效结论，不属于可回溯的历史批次")
    snapshot = storage.current_rules()
    snapshot_versions = {rv.spec.rule_id: rv.version for rv in snapshot}
    if previous.rule_versions == snapshot_versions:
        return previous, previous, "current"  # 结论依据的规则即当前规则：幂等跳过
    attempt = len(storage.conclusion_lineage(batch.batch_id)) + 1
    execution = Execution(
        execution_id=uuid.uuid4().hex,
        batch_id=batch.batch_id,
        attempt=attempt,
        rule_versions=snapshot_versions,
    )
    acquired, conflict_id, prior = storage.try_begin_rerun(execution)
    if acquired == "no-conclusion":
        raise BatchNotValidated(
            f"批次 {batch.batch_id!r} 尚无生效结论，不属于可回溯的历史批次")
    if acquired == "current":
        return prior, prior, "current"  # 并发方已抢先重跑完（或本就最新）：幂等跳过
    if acquired == "inflight":
        conflict = storage.get_execution(conflict_id) if conflict_id else None
        return (conflict if conflict is not None else previous), None, "inflight"
    try:
        if interrupt is not None:
            interrupt()
        findings = evaluate(batch.rows, snapshot)
        storage.commit_execution(execution.execution_id, findings,
                                 _final_status(findings))
    except BaseException:
        storage.abort_execution(execution.execution_id)
        raise
    return execution, prior, "ran"


def rerun_batch(storage: Storage, batch: Batch,
                interrupt: Callable[[], None] | None = None
                ) -> tuple[Execution, Execution | None, bool]:
    """按当前生效规则重跑单个批次。

    返回 (生效执行, 上一版执行, 是否实际重跑)：
    - (新执行, 上一版执行, True)：本次真正产生了新结论；
    - (当前执行, 当前执行, False)：本就按当前规则定论（CURRENT，幂等）；
    - (他人执行, 上一版执行, True)：与并发触发撞车，复用了同一份新结论。
    """
    while True:
        previous = storage.committed_execution(batch.batch_id)
        if previous is None:
            raise BatchNotValidated(
                f"批次 {batch.batch_id!r} 尚无生效结论，不属于可回溯的历史批次")
        snapshot_versions = {rv.spec.rule_id: rv.version
                             for rv in storage.current_rules()}
        if previous.rule_versions == snapshot_versions:
            return previous, previous, False

        effective, prior, state = _rerun_once(storage, batch, interrupt)
        if state == "ran":
            return effective, prior, True
        if state == "current":
            return effective, effective, False  # 幂等：最新结论已匹配当前规则
        # inflight：effective 是冲突的进行中执行，等待其落定
        settled = _wait_settled(storage, batch.batch_id, effective.execution_id)
        if settled is None:
            continue  # 冲突方中止：重新抓最新结论、重新抢权重试
        # 无论谁执行的，旧结论都是进入本轮前抓到的 previous
        return settled, previous, settled.execution_id != previous.execution_id


def backfill_batches(storage: Storage, batch_ids: list[str],
                     interrupts: dict[str, Callable[[], None]] | None = None
                     ) -> BackfillSummary:
    """批量回溯：逐批处理，单项失败被捕获并标记，不连累其它批次。

    顺序无关性：入参去重后按 batch_id 排序串行处理，每个批次的结论只取决于
    其自身数据与当前生效规则，因此以任何顺序回溯、甚至回溯与正常校验撞车，
    最终结论都与逐个处理完全一致。
    """
    targets = sorted(dict.fromkeys(batch_ids))
    if len(targets) > MAX_BACKFILL_BATCHES:
        raise ScopeExceeded(
            f"回溯批次 {len(targets)} 超过上界 {MAX_BACKFILL_BATCHES}，"
            "请分批回溯（不截断、不留半截结果）")
    # 不存在的批次在任何执行前整体拒绝，避免只给出半截批次的结果
    for batch_id in targets:
        if storage.get_batch(batch_id) is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在，无法回溯")

    interrupts = interrupts or {}
    items: list[BackfillItem] = []
    deltas = {"failure": 0, "warning": 0}
    for batch_id in targets:
        batch = storage.get_batch(batch_id)
        previous = storage.committed_execution(batch_id)
        try:
            if batch is not None and len(batch.rows) > MAX_BACKFILL_ROWS_PER_BATCH:
                raise ScopeExceeded(
                    f"批次 {batch_id!r} 行数 {len(batch.rows)} 超过单批上界 "
                    f"{MAX_BACKFILL_ROWS_PER_BATCH}，请缩小数据范围后单独重试该批次")
            effective, prior, ran = rerun_batch(
                storage, batch, interrupt=interrupts.get(batch_id))
        except Exception as exc:  # noqa: BLE001 - 单项失败隔离：标记 FAILED 并继续
            logger.warning("回溯失败: 批次=%s 原因=%s（旧结论仍生效，可单独重试）",
                           batch_id, exc)
            if previous is None:
                items.append(BackfillItem(
                    batch_id=batch_id, state=BackfillState.FAILED,
                    old_status=None, new_status=None, flipped=False,
                    diff=diffing.diff_findings([], []),
                    new_execution_id=None, error=str(exc)))
            else:
                status = _final_status(previous.findings)
                items.append(BackfillItem(
                    batch_id=batch_id, state=BackfillState.FAILED,
                    old_status=status, new_status=status, flipped=False,
                    diff=diffing.diff_findings(previous.findings, previous.findings),
                    new_execution_id=previous.execution_id, error=str(exc),
                    delta_by_severity={"failure": 0, "warning": 0}))
            continue

        if not ran:
            items.append(_item_skipped(batch_id, effective))
            logger.info("回溯跳过: 批次=%s 已按当前规则定论 execution=%s",
                        batch_id, effective.execution_id)
            continue

        old_findings = prior.findings if prior is not None else []
        old_status = (_final_status(old_findings) if prior is not None
                      else BatchStatus.REGISTERED)
        impact = diffing.impact_for(batch_id, old_status, old_findings,
                                    effective.findings)
        items.append(BackfillItem(
            batch_id=batch_id, state=BackfillState.UPDATED,
            old_status=impact.old_status, new_status=impact.new_status,
            flipped=impact.flipped, diff=impact.diff,
            new_execution_id=effective.execution_id,
            delta_by_severity=dict(impact.delta_by_severity)))
        for key in deltas:
            deltas[key] += impact.delta_by_severity.get(key, 0)
        logger.info("回溯更新: 批次=%s 旧结论=%s 新结论=%s 翻盘=%s "
                    "净变化(failure/warning)=%s/%s 新增=%d 消失=%d 同点变化=%d "
                    "旧execution=%s 新execution=%s",
                    batch_id, impact.old_status.value, impact.new_status.value,
                    impact.flipped, impact.delta_by_severity.get("failure", 0),
                    impact.delta_by_severity.get("warning", 0),
                    len(impact.diff.added), len(impact.diff.removed),
                    len(impact.diff.modified),
                    prior.execution_id if prior else None, effective.execution_id)

    summary = BackfillSummary(items=tuple(items), delta_by_severity=deltas)
    logger.info("回溯汇总: 批次=%d 更新=%d 跳过=%d 失败=%d 翻盘=%s "
                "级别净变化=%s",
                len(items), len(summary.updated),
                sum(1 for i in items if i.state is BackfillState.CURRENT),
                len(summary.failed),
                tuple(i.batch_id for i in items if i.flipped), deltas)
    return summary
