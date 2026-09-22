"""内置存储：规则版本、批次、执行记录与发现的线程安全存取。

并发一致性约定：
- 所有共享状态的读写都在同一把 RLock 下完成；
- 批次执行的"生效权"通过 try_begin_execution 的 compare-and-set 语义授予，
  保证同一批次并发/重复提交时只有一次执行生效；
- commit/abort 与状态推进在同一临界区内完成，不会留下半完成结果。
"""
from __future__ import annotations

import threading

from dqm.models import Batch, BatchStatus, Execution, Finding, RuleVersion


class Storage:
    """内置内存存储，所有写操作在单锁下完成以保证并发一致性。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rule_versions: dict[str, list[RuleVersion]] = {}
        self._batches: dict[str, Batch] = {}
        self._executions: dict[str, Execution] = {}          # execution_id -> Execution
        self._committed_by_batch: dict[str, str] = {}        # batch_id -> 生效 execution_id

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
        """
        with self._lock:
            batch = self._batches.get(execution.batch_id)
            if batch is None:
                return False, None
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
        """原子提交执行结果并推进批次状态。"""
        import time

        with self._lock:
            execution = self._executions[execution_id]
            execution.findings = list(findings)
            execution.status = "committed"
            execution.finished_at = time.time()
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
