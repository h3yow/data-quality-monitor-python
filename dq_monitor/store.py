"""线程安全的内置存储（模拟数据库）。

所有读写都在 RLock 内完成；执行结果要求"快照 + findings"在同一次
锁内原子可见——SUCCEEDED 之前 findings 对外始终为空，不会留下
半完成结果。状态流转用 CAS 保护（RUNNING -> SUCCEEDED/FAILED）。
"""

from __future__ import annotations

import threading

from .models import (
    Batch,
    Execution,
    ExecutionStatus,
    Finding,
    RuleStatus,
    RuleVersion,
    Schema,
    Stats,
    utcnow,
)


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.schemas: dict[str, Schema] = {}
        # rule_id -> 有序版本列表（index 0 为 v1，末位为最新）
        self.rules: dict[str, list[RuleVersion]] = {}
        self.batches: dict[str, Batch] = {}
        # batch_id -> Execution（一批次最多一条执行记录）
        self.executions: dict[str, Execution] = {}

    # ---- 锁辅助 ----
    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ---- schema ----
    def put_schema(self, schema: Schema) -> None:
        with self._lock:
            self.schemas[schema.name] = schema

    def get_schema(self, name: str) -> Schema:
        with self._lock:
            return self.schemas.get(name)

    # ---- rule ----
    def append_rule_version(self, rv: RuleVersion) -> None:
        """追加新版本，并把同 rule_id 的旧版本置为 SUPERSEDED。"""
        with self._lock:
            versions = self.rules.setdefault(rv.rule_id, [])
            for old in versions:
                old.status = RuleStatus.SUPERSEDED
            versions.append(rv)

    def get_rule_versions(self, rule_id: str) -> list[RuleVersion]:
        with self._lock:
            return list(self.rules.get(rule_id, []))

    def get_latest_rule(self, rule_id: str) -> RuleVersion | None:
        with self._lock:
            versions = self.rules.get(rule_id)
            return versions[-1] if versions else None

    def list_latest_rules(self) -> list[RuleVersion]:
        with self._lock:
            return [versions[-1] for versions in self.rules.values() if versions]

    def active_rules_for_schema(self, schema_name: str) -> list[RuleVersion]:
        """当前对某 schema 生效的规则（每个 rule_id 仅最新 ACTIVE 版本）。"""
        with self._lock:
            out: list[RuleVersion] = []
            for versions in self.rules.values():
                if not versions:
                    continue
                latest = versions[-1]
                if (
                    latest.status is RuleStatus.ACTIVE
                    and latest.schema_name == schema_name
                ):
                    out.append(latest)
            # 按 rule_id 排序，保证快照顺序确定
            out.sort(key=lambda r: r.rule_id)
            return out

    # ---- batch ----
    def put_batch(self, batch: Batch) -> None:
        with self._lock:
            self.batches[batch.batch_id] = batch

    def get_batch(self, batch_id: str) -> Batch:
        with self._lock:
            return self.batches.get(batch_id)

    # ---- execution ----
    def put_execution(self, execution: Execution) -> None:
        with self._lock:
            self.executions[execution.batch_id] = execution

    def get_execution(self, batch_id: str) -> Execution | None:
        with self._lock:
            return self.executions.get(batch_id)

    def create_running_if_absent(self, execution: Execution) -> Execution:
        """若该批次尚无执行记录，则放入 RUNNING 占位；否则返回已有记录。

        用于并发提交时的"占坑"：先到者创建，后来者拿到同一条记录。
        """
        with self._lock:
            existing = self.executions.get(execution.batch_id)
            if existing is not None:
                return existing
            self.executions[execution.batch_id] = execution
            return execution

    def mark_running_for_retry(self, batch_id: str) -> Execution | None:
        """CAS：FAILED -> RUNNING（attempt+1），供重试使用。"""
        with self._lock:
            execution = self.executions.get(batch_id)
            if execution is None or execution.status is not ExecutionStatus.FAILED:
                return None
            execution.status = ExecutionStatus.RUNNING
            execution.attempt += 1
            execution.started_at = utcnow()
            execution.fail_reason = None
            execution.findings = []
            return execution

    def commit_execution(
        self,
        batch_id: str,
        findings: list[Finding],
        checked_rows: int,
    ) -> Execution | None:
        """CAS：仅当当前状态为 RUNNING 时，原子写入结果并置 SUCCEEDED。

        findings 与终态在同一次锁内赋值，外部任何时刻要么看不到结果，
        要么看到完整结果，不存在半完成状态。
        """
        with self._lock:
            execution = self.executions.get(batch_id)
            if execution is None or execution.status is not ExecutionStatus.RUNNING:
                return None
            execution.findings = list(findings)
            execution.error_count = sum(
                1 for f in findings if f.severity.value == "ERROR"
            )
            execution.warn_count = sum(
                1 for f in findings if f.severity.value == "WARN"
            )
            execution.info_count = sum(
                1 for f in findings if f.severity.value == "INFO"
            )
            execution.checked_rows = checked_rows
            execution.status = ExecutionStatus.SUCCEEDED
            execution.finished_at = utcnow()
            execution.fail_reason = None
            return execution

    def fail_execution(self, batch_id: str, reason: str) -> Execution | None:
        """CAS：RUNNING -> FAILED。不写入任何 finding，结果保持为空。"""
        with self._lock:
            execution = self.executions.get(batch_id)
            if execution is None or execution.status is not ExecutionStatus.RUNNING:
                return None
            execution.findings = []
            execution.error_count = 0
            execution.warn_count = 0
            execution.info_count = 0
            execution.checked_rows = 0
            execution.status = ExecutionStatus.FAILED
            execution.finished_at = utcnow()
            execution.fail_reason = reason
            return execution

    def stats(self) -> Stats:
        with self._lock:
            executions = list(self.executions.values())
            return Stats(
                schemas=len(self.schemas),
                rules=len(self.rules),
                rule_versions=sum(len(v) for v in self.rules.values()),
                batches=len(self.batches),
                executions=len(executions),
                errors=sum(e.error_count for e in executions),
                warnings=sum(e.warn_count for e in executions),
                infos=sum(e.info_count for e in executions),
            )

    def reset(self) -> None:
        with self._lock:
            self.schemas.clear()
            self.rules.clear()
            self.batches.clear()
            self.executions.clear()
