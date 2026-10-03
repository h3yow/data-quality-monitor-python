"""内置存储：规则版本、批次、执行记录与发现的线程安全存取。

并发一致性约定：
- 所有共享状态的读写都在同一把 RLock 下完成；
- 批次执行的"生效权"通过 try_begin_execution 的 compare-and-set 语义授予，
  保证同一批次并发/重复提交时只有一次执行生效；
- 回溯（rerun）使用独立租约 _rerun_owner：提交前 _committed_by_batch 始终
  指向旧结论，查询在回溯进行中仍只能看到完整的旧结论，绝不暴露半完成结果；
- 结论以 Revision 修订版本追加留痕：正常执行产生 v1，回溯追加 v2/v3...，
  旧版本永不覆盖；
- commit/abort 与状态推进在同一临界区内完成，不会留下半完成结果。
"""
from __future__ import annotations

import threading

from dqm.models import (
    BackfillJob,
    Batch,
    BatchStatus,
    Execution,
    Finding,
    Revision,
    RuleVersion,
)


class Storage:
    """内置内存存储，所有写操作在单锁下完成以保证并发一致性。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rule_versions: dict[str, list[RuleVersion]] = {}
        self._batches: dict[str, Batch] = {}
        self._executions: dict[str, Execution] = {}          # execution_id -> Execution
        self._committed_by_batch: dict[str, str] = {}        # batch_id -> 生效 execution_id
        self._revisions: dict[str, list[Revision]] = {}      # batch_id -> 结论修订版本（追加）
        self._rerun_owner: dict[str, str] = {}               # batch_id -> 回溯租约 execution_id
        self._backfill_jobs: dict[str, BackfillJob] = {}     # job_id -> BackfillJob

    # ---- 规则 ----
    def current_version(self, rule_id: str) -> int:
        with self._lock:
            return len(self._rule_versions.get(rule_id, ()))

    def append_rule_version(self, rule: RuleVersion) -> None:
        with self._lock:
            self._rule_versions.setdefault(rule.spec.rule_id, []).append(rule)

    def get_rule_version(self, rule_id: str, version: int) -> RuleVersion | None:
        with self._lock:
            versions = self._rule_versions.get(rule_id, [])
            if 1 <= version <= len(versions):
                return versions[version - 1]
            return None

    def current_rules(self) -> list[RuleVersion]:
        with self._lock:
            return [versions[-1] for versions in self._rule_versions.values() if versions]

    # ---- 批次 ----
    def add_batch(self, batch: Batch) -> bool:
        """登记批次；batch_id 已存在返回 False（幂等去重）。"""
        with self._lock:
            if batch.batch_id in self._batches:
                return False
            self._batches[batch.batch_id] = batch
            return True

    def get_batch(self, batch_id: str) -> Batch | None:
        with self._lock:
            return self._batches.get(batch_id)

    # ---- 执行（幂等与并发控制核心）----
    def try_begin_execution(self, execution: Execution) -> tuple[bool, str | None]:
        """尝试让 execution 成为批次的生效执行（compare-and-set）。

        返回 (是否获得执行权, 已生效的 execution_id)。
        同一批次已有生效执行时，新执行不被接受，调用方应去重返回既有结果。
        批次被回溯租约占用时同样拒绝（租约提交前 _committed_by_batch 仍指向
        旧结论或为空，若不拦截，正常校验会与回溯并发执行、产生两条生效结论）。
        """
        with self._lock:
            batch = self._batches.get(execution.batch_id)
            if batch is None:
                return False, None
            rerun = self._rerun_owner.get(execution.batch_id)
            if rerun is not None:
                return False, rerun
            existing = self._committed_by_batch.get(execution.batch_id)
            if existing is not None:
                return False, existing
            # 生效权在 begin 时即登记，commit 只是补全结果；
            # 这样并发线程只能有一个进入执行体。
            self._committed_by_batch[execution.batch_id] = execution.execution_id
            self._executions[execution.execution_id] = execution
            batch.status = BatchStatus.RUNNING
            return True, execution.execution_id

    def commit_execution(self, execution_id: str, findings: list[Finding],
                         batch_status: BatchStatus) -> None:
        """原子提交执行结果并推进批次状态；结论作为修订版本 v1 留痕。"""
        import time

        with self._lock:
            execution = self._executions[execution_id]
            execution.findings = list(findings)
            execution.status = "committed"
            execution.finished_at = time.time()
            self._append_revision_locked(
                execution.batch_id, execution, findings, batch_status, "normal")
            batch = self._batches[execution.batch_id]
            batch.status = batch_status

    def abort_execution(self, execution_id: str) -> None:
        """执行中断：放弃执行权，批次状态恢复为 REGISTERED，不留半完成结果。"""
        import time

        with self._lock:
            execution = self._executions.pop(execution_id, None)
            if execution is None:
                return
            execution.status = "aborted"
            execution.finished_at = time.time()
            if self._committed_by_batch.get(execution.batch_id) == execution_id:
                del self._committed_by_batch[execution.batch_id]
            batch = self._batches.get(execution.batch_id)
            if batch is not None and batch.status == BatchStatus.RUNNING:
                batch.status = BatchStatus.REGISTERED

    def committed_execution(self, batch_id: str) -> Execution | None:
        with self._lock:
            execution_id = self._committed_by_batch.get(batch_id)
            if execution_id is None:
                return None
            return self._executions.get(execution_id)

    def get_execution(self, execution_id: str) -> Execution | None:
        with self._lock:
            return self._executions.get(execution_id)

    def list_batches(self) -> list[Batch]:
        """全部已登记批次，按 batch_id 升序（保证试算/批量回溯结果确定）。"""
        with self._lock:
            return [self._batches[k] for k in sorted(self._batches)]

    # ---- 结论修订版本（回溯留痕，旧结论永不覆盖）----
    def _append_revision_locked(self, batch_id: str, execution: Execution,
                                findings: list[Finding], batch_status: BatchStatus,
                                source: str) -> Revision:
        revisions = self._revisions.setdefault(batch_id, [])
        revision = Revision(
            revision_no=len(revisions) + 1,
            execution_id=execution.execution_id,
            rule_versions=dict(execution.rule_versions),
            status=batch_status,
            findings=tuple(findings),
            source=source,
        )
        revisions.append(revision)
        return revision

    def revisions(self, batch_id: str) -> list[Revision]:
        with self._lock:
            return list(self._revisions.get(batch_id, ()))

    def latest_revision(self, batch_id: str) -> Revision | None:
        with self._lock:
            revisions = self._revisions.get(batch_id)
            return revisions[-1] if revisions else None

    # ---- 回溯（rerun）：独立租约 + 原子提交/放弃 ----
    def acquire_rerun_if_stale(self, execution: Execution,
                               target_signature: tuple[tuple[str, int], ...],
                               sign) -> str:
        """在同一临界区内完成"是否需要重跑 + 抢回溯租约"，消除检查-执行竞态。

        返回：
        - "acquired"：调用方获得租约，应真正执行；
        - "current" ：最新结论已是目标快照，无需重跑（调用方直接去重）；
        - "busy"    ：批次有执行在身（并发回溯/正常校验），调用方应等待后重判。
        """
        with self._lock:
            batch = self._batches.get(execution.batch_id)
            if batch is None:
                return "busy"
            revisions = self._revisions.get(execution.batch_id, ())
            latest = revisions[-1] if revisions else None
            if latest is not None and sign(latest.rule_versions) == target_signature:
                return "current"
            if execution.batch_id in self._rerun_owner:
                return "busy"
            if batch.status is BatchStatus.RUNNING:
                return "busy"
            self._rerun_owner[execution.batch_id] = execution.execution_id
            self._executions[execution.execution_id] = execution
            batch.status = BatchStatus.RUNNING
            return "acquired"

    def commit_rerun(self, execution_id: str, findings: list[Finding],
                     batch_status: BatchStatus) -> Revision:
        """原子提交回溯结果：追加结论修订版本、切换生效结论、推进批次状态。"""
        import time

        with self._lock:
            execution = self._executions[execution_id]
            execution.findings = list(findings)
            execution.status = "committed"
            execution.finished_at = time.time()
            revision = self._append_revision_locked(
                execution.batch_id, execution, findings, batch_status, "backfill")
            self._committed_by_batch[execution.batch_id] = execution_id
            self._rerun_owner.pop(execution.batch_id, None)
            self._batches[execution.batch_id].status = batch_status
            return revision

    def abort_rerun(self, execution_id: str) -> None:
        """回溯中断：放弃租约与执行记录，恢复旧结论状态，不留半完成结果。"""
        import time

        with self._lock:
            execution = self._executions.pop(execution_id, None)
            if execution is None:
                return
            execution.status = "aborted"
            execution.finished_at = time.time()
            if self._rerun_owner.get(execution.batch_id) == execution_id:
                del self._rerun_owner[execution.batch_id]
            batch = self._batches.get(execution.batch_id)
            if batch is not None and batch.status == BatchStatus.RUNNING:
                # 恢复为旧结论状态（曾失败的批次仍为 failed）
                latest = self.latest_revision(execution.batch_id)
                batch.status = latest.status if latest else BatchStatus.REGISTERED

    def rerun_owner(self, batch_id: str) -> str | None:
        with self._lock:
            return self._rerun_owner.get(batch_id)

    # ---- 回溯作业账本 ----
    def save_backfill_job(self, job: BackfillJob) -> None:
        with self._lock:
            self._backfill_jobs[job.job_id] = job

    def get_backfill_job(self, job_id: str) -> BackfillJob | None:
        with self._lock:
            return self._backfill_jobs.get(job_id)


class ReadOnlyStorageView:
    """Storage 的只读视图：只暴露读取接口。

    影响试算全程只持有该视图，物理上无法触达任何写操作，
    从结构上保证试算"绝对不会顺手改掉生效规则或历史结论"。
    """

    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    def current_version(self, rule_id: str) -> int:
        return self._storage.current_version(rule_id)

    def get_rule_version(self, rule_id: str, version: int) -> RuleVersion | None:
        return self._storage.get_rule_version(rule_id, version)

    def current_rules(self) -> list[RuleVersion]:
        return self._storage.current_rules()

    def get_batch(self, batch_id: str) -> Batch | None:
        return self._storage.get_batch(batch_id)

    def list_batches(self) -> list[Batch]:
        return self._storage.list_batches()

    def get_execution(self, execution_id: str) -> Execution | None:
        return self._storage.get_execution(execution_id)

    def committed_execution(self, batch_id: str) -> Execution | None:
        return self._storage.committed_execution(batch_id)

    def revisions(self, batch_id: str) -> list[Revision]:
        return self._storage.revisions(batch_id)

    def latest_revision(self, batch_id: str) -> Revision | None:
        return self._storage.latest_revision(batch_id)

    def get_backfill_job(self, job_id: str) -> BackfillJob | None:
        return self._storage.get_backfill_job(job_id)
