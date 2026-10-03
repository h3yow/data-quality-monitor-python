"""历史批次回溯：按当前生效规则整体重跑，结论版本化保留，支持并发去重与中断重试。

语义约定：
- 结论版本化：每次真正重跑追加新的 Revision（source=backfill），
  旧结论永不覆盖，可通过 revisions/compare_latest 并排对照；
- 唯一生效：同一批次的回溯被重复/并发提交时，存储层在同一临界区内完成
  "是否已最新 + 抢租约"判定（acquire_rerun_if_stale），只放一个执行体进入；目标规则快照若已与最新结论一致，
  直接去重返回既有版本（reran=False），不重复执行、不重复计数；
- 与正常校验互斥：回溯租约占用期间，正常 validate 会等待其完成并拿到
  最新结论；反之亦然，两种触发方式最终结论对得上，同一批次绝不出现
  两条生效结论；
- 等待方自愈：等待中的重复/并发调用若遇到占用方被中断（abort），
  不会把内部错误抛给调用方，而是重新竞争执行权自己补跑，
  最终收敛出唯一一份生效结论，批次不会卡在未执行状态；
- 失败隔离：批量回溯逐个批次记账，单个批次失败只把该条目标记 failed，
  不连累其它批次、不留下半完成结论，可单独/按作业重试到成功；
- 有界处理：单次作业批次数、并发度均有上界，最终结论与逐个处理一致，
  且与提交顺序无关（结果按 batch_id 升序）。
"""
from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from dqm.errors import BackfillError, BatchNotFound, WorkloadTooLarge
from dqm.executor import (
    MAX_ROWS_PER_BATCH,
    BatchTooLarge,
    evaluate,
    final_status,
    wait_for_effective_execution,
)
from dqm.models import (
    BackfillItem,
    BackfillItemStatus,
    BackfillJob,
    Batch,
    Execution,
    Revision,
    RevisionComparison,
)
from dqm.storage import Storage
from dqm.trial import findings_delta

logger = logging.getLogger("dqm.backfill")

# 处理上界：批量回溯的批次数与并发度；结论不允许因为基数大而失控。
MAX_BACKFILL_BATCHES = 10_000
DEFAULT_WORKERS = 4
MAX_WORKERS = 8
# 等待占用方（正常校验/并发回溯）完成的最大轮询次数（每次 5ms），防止异常竞态死循环
_WAIT_SPINS = 6000


def snapshot_signature(rule_versions: dict[str, int]) -> tuple[tuple[str, int], ...]:
    """规则快照的规范化指纹（按 rule_id 排序），用于判断结论是否已是最新。"""
    return tuple(sorted(rule_versions.items()))


def backfill_batch(
    storage: Storage,
    batch: Batch,
    interrupt: Callable[[], None] | None = None,
) -> tuple[Revision, bool]:
    """对单个批次按当前生效规则回溯重跑。

    返回 (最新结论版本, 本次调用是否真正重跑)。定心循环保证：
    - 目标快照与最新结论一致：去重返回既有版本，reran=False；
    - 同一批次已有回溯在执行，或正常校验在执行：等待其完成后回到循环顶部，
      按最新状态重新判定（唯一生效，绝不两个执行体并发）。
    """
    if len(batch.rows) > MAX_ROWS_PER_BATCH:
        raise WorkloadTooLarge(
            f"批次 {batch.batch_id!r} 行数 {len(batch.rows)} 超过单批上界 "
            f"{MAX_ROWS_PER_BATCH}，回溯拒绝")
    for _ in range(_WAIT_SPINS):
        snapshot = storage.current_rules()  # 每轮重新取快照：等待期间规则可能变更
        target = snapshot_signature({rv.spec.rule_id: rv.version for rv in snapshot})

        execution = Execution(
            execution_id=uuid.uuid4().hex,
            batch_id=batch.batch_id,
            attempt=1,
            rule_versions={rv.spec.rule_id: rv.version for rv in snapshot},
        )
        # "是否已最新 + 抢租约"在同一临界区内完成，杜绝多个线程同时判定
        # "需要重跑"后各自执行一次的竞态
        decision = storage.acquire_rerun_if_stale(
            execution, target, snapshot_signature)
        if decision == "current":
            latest = storage.latest_revision(batch.batch_id)
            logger.info("回溯去重 批次=%s：最新结论已是当前规则快照 %s，无需重跑",
                        batch.batch_id, dict(target))
            return latest, False
        if decision == "busy":
            # 并发回溯或正常校验占着该批次：等其尘埃落定后回到循环顶部重新判定。
            # 占用方提交 -> 下一轮按"已最新"去重；占用方中断退出（返回 None）
            # -> 下一轮由本调用自己抢到租约补跑，批次不会卡在未执行状态。
            logger.info("回溯等待 批次=%s：该批次正有执行在身，等待其完成",
                        batch.batch_id)
            wait_for_effective_execution(storage, batch.batch_id)
            continue

        try:
            if interrupt is not None:
                interrupt()  # 测试注入的中断点：模拟回溯中途崩溃
            logger.info("回溯执行 批次=%s 行数=%d 判定依据 rule_versions=%s",
                        batch.batch_id, len(batch.rows), dict(target))
            findings = evaluate(batch.rows, snapshot)
            revision = storage.commit_rerun(
                execution.execution_id, findings, final_status(findings))
        except BatchTooLarge:
            storage.abort_rerun(execution.execution_id)
            raise WorkloadTooLarge(
                f"批次 {batch.batch_id!r} 行数 {len(batch.rows)} 超过单批上界 "
                f"{MAX_ROWS_PER_BATCH}，回溯拒绝（不留半完成结果）")
        except BaseException:
            storage.abort_rerun(execution.execution_id)
            raise
        logger.info("回溯提交 批次=%s -> revision=%d status=%s failures=%d warnings=%d",
                    batch.batch_id, revision.revision_no, revision.status.value,
                    sum(1 for f in findings if f.severity.value == "failure"),
                    sum(1 for f in findings if f.severity.value == "warning"))
        return revision, True

    raise TimeoutError(f"批次 {batch.batch_id} 回溯等待占用方释放超时")


def compare_latest(
    storage: Storage,
    batch_id: str,
    reran: bool | None = None,
) -> RevisionComparison:
    """并排对照批次最新两个结论版本；只有一版时 previous 为 None。"""
    revisions = storage.revisions(batch_id)
    if not revisions:
        raise BackfillError(f"批次 {batch_id!r} 尚无任何结论，无法对照")
    current = revisions[-1]
    previous = revisions[-2] if len(revisions) >= 2 else None
    previous_findings = previous.findings if previous is not None else ()
    added, removed = findings_delta(previous_findings, current.findings)
    if reran is None:
        # 默认按快照是否变化判断；调用方也可显式传入本次是否真正重跑
        reran = previous is None or previous.rule_versions != current.rule_versions
    return RevisionComparison(
        batch_id=batch_id,
        previous_revision_no=previous.revision_no if previous else None,
        current_revision_no=current.revision_no,
        previous_status=previous.status if previous else None,
        current_status=current.status,
        flipped=bool(previous and previous.status is not current.status),
        reran=reran,
        added=added,
        removed=removed,
    )


def _run_one_item(storage: Storage, job: BackfillJob, batch_id: str,
                  interrupt: Callable[[], None] | None = None) -> None:
    """作业内处理单个批次：任何失败只落到该条目的状态与错误信息上。"""
    item = job.items[batch_id]
    item.status = BackfillItemStatus.RUNNING
    item.error = None
    storage.save_backfill_job(job)
    try:
        batch = storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id!r} 不存在")
        revision, reran = backfill_batch(storage, batch, interrupt=interrupt)
        item.status = BackfillItemStatus.SUCCEEDED
        item.revision_no = revision.revision_no
        item.reran = reran  # 如实记账：真重跑产出新结论 vs 快照已最新被去重
        item.error = None
        item_was = "重跑" if reran else "已最新(去重)"
        logger.info("回溯作业=%s 批次=%s 成功[%s] revision=%s",
                    job.job_id, batch_id, item_was, revision.revision_no)
    except Exception as exc:  # noqa: BLE001 - 失败隔离：记录后继续其它批次
        item.status = BackfillItemStatus.FAILED
        item.error = str(exc)
        latest = storage.latest_revision(batch_id)
        item.revision_no = latest.revision_no if latest else None
        logger.warning("回溯作业=%s 批次=%s 失败（不连累其它批次）: %s",
                       job.job_id, batch_id, exc)
    finally:
        storage.save_backfill_job(job)


def _finish_job(storage: Storage, job: BackfillJob) -> None:
    if all(item.status is BackfillItemStatus.SUCCEEDED for item in job.items.values()):
        job.finished_at = time.time()
    storage.save_backfill_job(job)


def backfill_batches(
    storage: Storage,
    batch_ids: list[str],
    max_workers: int | None = None,
) -> BackfillJob:
    """批量回溯：逐批次记账，失败隔离；返回作业账本（可后续重试未成功条目）。"""
    if not batch_ids:
        raise BackfillError("回溯批次列表为空")
    missing = [bid for bid in batch_ids if storage.get_batch(bid) is None]
    if missing:
        raise BackfillError(f"批次不存在，无法回溯: {missing}")
    oversized = [bid for bid in set(batch_ids)
                 if len(storage.get_batch(bid).rows) > MAX_ROWS_PER_BATCH]
    if oversized:
        raise WorkloadTooLarge(
            f"批次行数超过单批上界 {MAX_ROWS_PER_BATCH}，回溯拒绝: {sorted(oversized)}")
    if len(batch_ids) > MAX_BACKFILL_BATCHES:
        raise BackfillError(
            f"单次回溯批次数 {len(batch_ids)} 超过上界 {MAX_BACKFILL_BATCHES}，"
            f"请拆分后提交")

    ordered = sorted(set(batch_ids))  # 去重 + 固定顺序：与提交顺序无关
    workers = max(1, min(max_workers or DEFAULT_WORKERS, MAX_WORKERS))
    job = BackfillJob(
        job_id=uuid.uuid4().hex,
        items={bid: BackfillItem(batch_id=bid) for bid in ordered},
    )
    storage.save_backfill_job(job)
    logger.info("批量回溯开始 job=%s 批次数=%d 并发=%d 顺序=%s",
                job.job_id, len(ordered), workers, ordered)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_run_one_item, storage, job, bid) for bid in ordered]
        for future in futures:
            future.result()  # 异常已在 _run_one_item 内隔离，这里仅等待完成

    _finish_job(storage, job)
    succeeded = len(job.items_by_status(BackfillItemStatus.SUCCEEDED))
    failed = len(job.items_by_status(BackfillItemStatus.FAILED))
    logger.info("批量回溯结束 job=%s 成功=%d 失败=%d", job.job_id, succeeded, failed)
    return job


def retry_backfill_job(storage: Storage, job_id: str) -> BackfillJob:
    """重试作业中所有未成功的批次（failed/pending）；已成功条目不重跑。"""
    job = storage.get_backfill_job(job_id)
    if job is None:
        raise BackfillError(f"回溯作业 {job_id!r} 不存在")
    pending = [bid for bid, item in job.items.items()
               if item.status is not BackfillItemStatus.SUCCEEDED]
    logger.info("重试回溯作业=%s 待重试批次=%s", job_id, pending)
    # 顺序固定，仍在有界并发内逐个重试；单批次再次失败不连累其它批次
    ordered = sorted(pending)
    workers = min(DEFAULT_WORKERS, MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_run_one_item, storage, job, bid) for bid in ordered]
        for future in futures:
            future.result()
    _finish_job(storage, job)
    return job
