"""内置存储：规则版本、批次、执行记录与发现的线程安全存取。

并发一致性约定：
- 所有共享状态的读写都在同一把 RLock 下完成；
- 批次执行的"生效权"通过 try_begin_execution 的 compare-and-set 语义授予，
  保证同一批次并发/重复提交时只有一次执行生效；
- 执行权分两层登记：_inflight_by_batch（running，含回溯重跑）与
  _committed_by_batch（已落定的最新结论）。任何触发（正常校验或回溯）都要先过
  inflight 这道 CAS，因此同一批次无论从哪条路径并发进入，都只有一次真正执行；
- commit/abort 与状态推进在同一临界区内完成，不会留下半完成结果；
- 回溯提交时旧结论不覆盖、不抹除：全部已提交执行按顺序进入 _lineage_by_batch，
  最新一条即当前结论，历史结论随时可查、可对照。
"""
from __future__ import annotations

import threading

from dqm.models import Batch, BatchStatus, Execution, Finding, RuleVersion


def _new_event() -> threading.Event:
    return threading.Event()


class Storage:
    """内置内存存储，所有写操作在单锁下完成以保证并发一致性。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rule_versions: dict[str, list[RuleVersion]] = {}
        self._batches: dict[str, Batch] = {}
        self._executions: dict[str, Execution] = {}            # execution_id -> Execution
        self._inflight_by_batch: dict[str, str] = {}           # batch_id -> 进行中 execution_id
        self._committed_by_batch: dict[str, str] = {}          # batch_id -> 最新生效 execution_id
        self._lineage_by_batch: dict[str, list[str]] = {}      # batch_id -> 历版 execution_id（按提交顺序）

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

    def all_batches(self) -> list[Batch]:
        """全部已登记批次（返回快照副本，调用方可安全遍历）。"""
        with self._lock:
            return list(self._batches.values())

    # ---- 执行（幂等与并发控制核心）----
    def try_begin_execution(self, execution: Execution) -> tuple[bool, str | None]:
        """尝试让 execution 开始执行批次的首次校验（compare-and-set）。

        返回 (是否获得执行权, 冲突的 execution_id)：
        - 批次已有进行中的执行（含回溯重跑）：冲突方为该 inflight execution，
          调用方等待其落定后取最新结论；
        - 批次已有已提交结论：冲突方为该 committed execution，调用方直接去重返回；
        - 否则登记为 inflight，批次转 RUNNING，只有一个调用方能进入执行体。
        """
        with self._lock:
            batch = self._batches.get(execution.batch_id)
            if batch is None:
                return False, None
            inflight = self._inflight_by_batch.get(execution.batch_id)
            if inflight is not None:
                return False, inflight
            committed = self._committed_by_batch.get(execution.batch_id)
            if committed is not None:
                return False, committed
            execution.settled_event = _new_event()
            self._executions[execution.execution_id] = execution
            self._inflight_by_batch[execution.batch_id] = execution.execution_id
            batch.status = BatchStatus.RUNNING
            return True, execution.execution_id

    def try_begin_rerun(self, execution: Execution
                        ) -> tuple[str, str | None, Execution | None]:
        """回溯重跑入口。返回 (结果码, 关联 execution_id, 关联执行)。

        结果码：
        - "acquired"：本调用获得执行权，关联执行即上一版生效结论（供对照）；
        - "inflight"：另有执行（正常校验或回溯）在跑，关联其 execution，等待落定；
        - "current"：最新结论依据的规则快照与本次一致（并发方已抢先重跑完，
          或本就按当前规则定论），关联该最新执行，调用方幂等跳过，不重复跑；
        - "no-conclusion"：批次不存在或尚无生效结论，不可回溯。
        """
        with self._lock:
            batch = self._batches.get(execution.batch_id)
            if batch is None:
                return "no-conclusion", None, None
            inflight = self._inflight_by_batch.get(execution.batch_id)
            if inflight is not None:
                return "inflight", inflight, None
            committed_id = self._committed_by_batch.get(execution.batch_id)
            if committed_id is None:
                return "no-conclusion", None, None
            committed = self._executions[committed_id]
            if committed.rule_versions == execution.rule_versions:
                return "current", committed_id, committed
            execution.settled_event = _new_event()
            self._executions[execution.execution_id] = execution
            self._inflight_by_batch[execution.batch_id] = execution.execution_id
            batch.status = BatchStatus.RUNNING
            return "acquired", execution.execution_id, committed

    def commit_execution(self, execution_id: str, findings: list[Finding],
                         batch_status: BatchStatus) -> None:
        """原子提交执行结果并推进批次状态；结论进入谱系，旧结论保留不覆盖。"""
        import time

        with self._lock:
            execution = self._executions[execution_id]
            execution.findings = list(findings)
            execution.status = "committed"
            execution.finished_at = time.time()
            self._inflight_by_batch.pop(execution.batch_id, None)
            self._committed_by_batch[execution.batch_id] = execution_id
            self._lineage_by_batch.setdefault(execution.batch_id, []).append(execution_id)
            batch = self._batches[execution.batch_id]
            batch.status = batch_status
            event = execution.settled_event
        if event is not None:  # 通知必须在锁外发出，避免唤醒方立即抢锁造成无谓阻塞
            event.set()

    def abort_execution(self, execution_id: str) -> None:
        """执行中断：放弃执行权，批次恢复到上一版结论的状态，不留半完成结果。"""
        import time

        with self._lock:
            execution = self._executions.get(execution_id)
            if execution is None:
                return
            execution.status = "aborted"
            execution.finished_at = time.time()
            # 中止记录保留在 _executions 中（标记 aborted，绝不作为结论），
            # 以便等待方识别"这一跑失败了，需要重试"。
            if self._inflight_by_batch.get(execution.batch_id) == execution_id:
                del self._inflight_by_batch[execution.batch_id]
            batch = self._batches.get(execution.batch_id)
            if batch is not None and batch.status == BatchStatus.RUNNING:
                committed_id = self._committed_by_batch.get(execution.batch_id)
                if committed_id is not None and committed_id != execution_id:
                    prior = self._executions[committed_id]
                    batch.status = (BatchStatus.FAILED
                                    if any(f.severity.value == "failure"
                                           for f in prior.findings)
                                    else BatchStatus.SUCCEEDED)
                else:
                    batch.status = BatchStatus.REGISTERED
            event = execution.settled_event
        if event is not None:
            event.set()

    def get_execution(self, execution_id: str) -> Execution | None:
        with self._lock:
            return self._executions.get(execution_id)

    def committed_execution(self, batch_id: str) -> Execution | None:
        with self._lock:
            execution_id = self._committed_by_batch.get(batch_id)
            if execution_id is None:
                return None
            return self._executions.get(execution_id)

    def conclusion_lineage(self, batch_id: str) -> list[Execution]:
        """批次的全部历史生效结论（按提交先后，最旧在前；不含 aborted 尝试）。"""
        with self._lock:
            return [self._executions[eid]
                    for eid in self._lineage_by_batch.get(batch_id, ())
                    if eid in self._executions]
