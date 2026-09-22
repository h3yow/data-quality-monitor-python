"""应用服务层：登记、提交执行、重试、查询与复现的编排。

并发与幂等模型：
- 每个 batch_id 一把互斥锁（_lock_for），同批次的提交串行化；
- 存储层用 CAS 占坑（create_running_if_absent），因此即便不同线程
  近乎同时提交，也只有一个线程得到新建的 RUNNING 记录并真正执行，
  另一线程拿到同一条记录并观察其终态；
- 执行结果（snapshot + findings + 计数 + SUCCEEDED）在同一把存储锁内
  原子提交，任何观察者要么看不到结果，要么看到完整结果；
- SUCCEEDED 是终态：重复提交直接返回，不重复执行、不重复计数；
- FAILED 可恢复：mark_running_for_retry 复用同一执行记录，attempt+1，
  成功后原子覆盖为空的旧结果。
"""

from __future__ import annotations

import logging
import threading
import uuid

from .engine import ExecutionCrashed, run_execution
from .models import (
    Batch,
    DQError,
    ErrorCode,
    Execution,
    ExecutionStatus,
    FieldSpec,
    Finding,
    RuleStatus,
    RuleVersion,
    Schema,
    Severity,
    Stats,
    utcnow,
)
from .store import Store
from .validators import (
    detect_conflicts,
    validate_field_specs,
    validate_rule_definition,
    value_matches_dtype,
)

logger = logging.getLogger("dq.service")

# 批次行中以下划线开头的键为测试控制元数据，不参与字段校验/求值
_META_KEYS = {"__crash__", "__delay__"}


class DataQualityService:
    def __init__(self, store: Store | None = None) -> None:
        self.store = store or Store()
        self._batch_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _lock_for(self, batch_id: str) -> threading.Lock:
        with self._locks_guard:
            lk = self._batch_locks.get(batch_id)
            if lk is None:
                lk = threading.Lock()
                self._batch_locks[batch_id] = lk
            return lk

    # ============================================================== #
    # 登记
    # ============================================================== #
    def register_schema(self, name: str, fields: list[dict]) -> Schema:
        logger.info("登记 schema name=%s fields=%s", name, fields)
        if not name or not isinstance(name, str):
            raise DQError(ErrorCode.RULE_PARAM_MISSING, "schema 名称缺失", {"name": name})
        validate_field_specs(fields)
        with self.store.lock:
            if self.store.get_schema(name) is not None:
                raise DQError(
                    ErrorCode.SCHEMA_EXISTS,
                    f"schema {name} 已存在",
                    {"schema_name": name},
                )
            schema = Schema(
                name=name,
                fields={
                    f["name"]: FieldSpec(
                        f["name"], f["dtype"], f.get("nullable", True)
                    )
                    for f in fields
                },
            )
            self.store.put_schema(schema)
        logger.info("schema 已登记 name=%s field_count=%d", name, len(schema.fields))
        return schema

    def register_rule(
        self,
        rule_id: str,
        schema_name: str,
        field_name: str,
        rule_type: str,
        severity: str,
        params: dict,
    ) -> RuleVersion:
        logger.info(
            "登记规则 rule_id=%s schema=%s field=%s type=%s severity=%s params=%s",
            rule_id, schema_name, field_name, rule_type, severity, params,
        )
        if not rule_id or not isinstance(rule_id, str):
            raise DQError(ErrorCode.RULE_PARAM_MISSING, "rule_id 缺失", {"rule_id": rule_id})
        schema = self.store.get_schema(schema_name)
        if schema is None:
            raise DQError(
                ErrorCode.SCHEMA_NOT_FOUND,
                f"schema {schema_name} 不存在",
                {"schema_name": schema_name},
            )
        try:
            sev = Severity(severity)
        except ValueError:
            raise DQError(
                ErrorCode.RULE_PARAM_INVALID,
                f"severity {severity!r} 非法，允许: {[s.value for s in Severity]}",
                {"severity": severity},
            ) from None

        rtype = validate_rule_definition(rule_type, params or {}, schema, field_name)

        with self.store.lock:
            versions = self.store.get_rule_versions(rule_id)
            if versions:
                latest_self = versions[-1]
                # rule_id 是规则的全局身份，新版本只能指向同一 schema/字段
                if (
                    latest_self.schema_name != schema_name
                    or latest_self.field_name != field_name
                ):
                    raise DQError(
                        ErrorCode.RULE_CONFLICT,
                        f"rule_id {rule_id} 已绑定到 schema "
                        f"{latest_self.schema_name}.{latest_self.field_name}，"
                        "不能改绑到其它 schema/字段；请使用新的 rule_id",
                        {
                            "rule_id": rule_id,
                            "reason": "rule_id_target_changed",
                            "bound_to": {
                                "schema_name": latest_self.schema_name,
                                "field_name": latest_self.field_name,
                            },
                        },
                    )
                if latest_self.rule_type is rtype and latest_self.params == (params or {}):
                    raise DQError(
                        ErrorCode.RULE_CONFLICT,
                        f"规则 {rule_id} 最新版本 v{latest_self.version} 与提交内容完全"
                        "一致，未发生变更（重复提交请直接复用现有版本）",
                        {"rule_id": rule_id, "version": latest_self.version,
                         "reason": "no_change"},
                    )
            candidate = {"rule_id": rule_id, "rule_type": rtype, "params": params or {},
                         "field_name": field_name}
            # 同字段同级别的其它生效规则做冲突检查（自身旧版本已另作处理）
            existing: list[dict] = []
            for rv in self.store.active_rules_for_schema(schema_name):
                if rv.field_name != field_name or rv.severity is not sev:
                    continue
                if rv.rule_id == rule_id:
                    continue
                existing.append({"rule_id": rv.rule_id, "rule_type": rv.rule_type,
                                 "params": rv.params, "field_name": rv.field_name})
            detect_conflicts(candidate, existing)

            version = len(versions) + 1
            rv = RuleVersion(
                rule_id=rule_id,
                version=version,
                schema_name=schema_name,
                field_name=field_name,
                rule_type=rtype,
                severity=sev,
                params=dict(params or {}),
                status=RuleStatus.ACTIVE,
            )
            self.store.append_rule_version(rv)
        logger.info("规则已登记 rule_id=%s version=%d", rule_id, rv.version)
        return rv

    def register_batch(self, batch_id: str, schema_name: str, rows: list[dict]) -> Batch:
        logger.info("登记批次 batch_id=%s schema=%s row_count=%d", batch_id, schema_name, len(rows))
        if not batch_id or not isinstance(batch_id, str):
            raise DQError(ErrorCode.RULE_PARAM_MISSING, "batch_id 缺失", {"batch_id": batch_id})
        schema = self.store.get_schema(schema_name)
        if schema is None:
            raise DQError(
                ErrorCode.SCHEMA_NOT_FOUND,
                f"schema {schema_name} 不存在",
                {"schema_name": schema_name},
            )
        if not isinstance(rows, list) or not rows:
            raise DQError(
                ErrorCode.BATCH_EMPTY,
                f"批次 {batch_id} 为空，至少需要一行数据",
                {"batch_id": batch_id},
            )

        with self.store.lock:
            if self.store.get_batch(batch_id) is not None:
                raise DQError(
                    ErrorCode.BATCH_EXISTS,
                    f"批次 {batch_id} 已登记",
                    {"batch_id": batch_id},
                )
            for idx, row in enumerate(rows):
                if not isinstance(row, dict):
                    raise DQError(
                        ErrorCode.BATCH_TYPE_MISMATCH,
                        f"第 {idx} 行不是对象",
                        {"row_index": idx},
                    )
                for fname, spec in schema.fields.items():
                    if fname not in row:
                        raise DQError(
                            ErrorCode.BATCH_FIELD_MISSING,
                            f"第 {idx} 行缺失字段 {fname}",
                            {"row_index": idx, "field_name": fname},
                        )
                    value = row[fname]
                    if not spec.nullable and value is None:
                        raise DQError(
                            ErrorCode.BATCH_TYPE_MISMATCH,
                            f"第 {idx} 行字段 {fname} 声明非空但值为 null",
                            {"row_index": idx, "field_name": fname},
                        )
                    if not value_matches_dtype(value, spec.dtype):
                        raise DQError(
                            ErrorCode.BATCH_TYPE_MISMATCH,
                            f"第 {idx} 行字段 {fname} 的值 {value!r} 与类型 "
                            f"{spec.dtype} 不匹配",
                            {"row_index": idx, "field_name": fname,
                             "dtype": spec.dtype, "value": value},
                        )
            batch = Batch(batch_id=batch_id, schema_name=schema_name, rows=list(rows))
            self.store.put_batch(batch)
        logger.info("批次已登记 batch_id=%s rows=%d", batch_id, len(rows))
        return batch

    # ============================================================== #
    # 执行
    # ============================================================== #
    def submit(self, batch_id: str) -> tuple[Execution, bool]:
        batch = self.store.get_batch(batch_id)
        if batch is None:
            raise DQError(
                ErrorCode.BATCH_NOT_FOUND,
                f"批次 {batch_id} 不存在",
                {"batch_id": batch_id},
            )

        with self._lock_for(batch_id):
            existing = self.store.get_execution(batch_id)
            if existing is None:
                # 首次提交：快照当前生效规则 + CAS 占坑
                active = self.store.active_rules_for_schema(batch.schema_name)
                execution = Execution(
                    execution_id=str(uuid.uuid4()),
                    batch_id=batch_id,
                    status=ExecutionStatus.RUNNING,
                    rule_snapshot=[r.snapshot() for r in active],
                    findings=[],
                    error_count=0, warn_count=0, info_count=0,
                    checked_rows=0, attempt=1,
                    started_at=utcnow(),
                )
                winner = self.store.create_running_if_absent(execution)
                if winner is execution:
                    self._execute(execution, batch)
                    return self.store.get_execution(batch_id), True
                # per-batch 锁内理论上不会走到这里；防御性返回已有记录
                return winner, False

            if existing.status is ExecutionStatus.SUCCEEDED:
                logger.info("批次 %s 已成功执行，重复提交直接返回，不重复计数", batch_id)
                return existing, False
            if existing.status is ExecutionStatus.RUNNING:
                logger.info("批次 %s 正在执行中，并发提交返回同一执行记录", batch_id)
                return existing, False
            # FAILED -> 重试
            retried = self.store.mark_running_for_retry(batch_id)
            if retried is None:
                return self.store.get_execution(batch_id), False
            logger.info("批次 %s 上次执行中断，开始第 %d 次尝试", batch_id, retried.attempt)
            self._execute(retried, batch)
            return self.store.get_execution(batch_id), True

    def retry(self, batch_id: str) -> Execution:
        batch = self.store.get_batch(batch_id)
        if batch is None:
            raise DQError(ErrorCode.BATCH_NOT_FOUND, f"批次 {batch_id} 不存在",
                          {"batch_id": batch_id})
        # 状态判定 + CAS + 执行全部在同一把批次锁内：并发重试串行化，
        # 只有一个线程看到 FAILED 并发起重试，其余看到终态/运行中，语义确定。
        with self._lock_for(batch_id):
            execution = self.store.get_execution(batch_id)
            if execution is None:
                raise DQError(
                    ErrorCode.EXECUTION_NOT_FOUND,
                    f"批次 {batch_id} 尚无执行记录，请先提交",
                    {"batch_id": batch_id},
                )
            if execution.status is ExecutionStatus.SUCCEEDED:
                raise DQError(
                    ErrorCode.RULE_CONFLICT,
                    f"批次 {batch_id} 已成功完成，终态不可重试",
                    {"batch_id": batch_id, "reason": "terminal_succeeded"},
                )
            if execution.status is ExecutionStatus.RUNNING:
                raise DQError(
                    ErrorCode.EXECUTION_RUNNING,
                    f"批次 {batch_id} 正在执行中，不能重复发起重试",
                    {"batch_id": batch_id},
                )
            retried = self.store.mark_running_for_retry(batch_id)
            if retried is None:  # 防御：锁内理论上不会发生
                raise DQError(
                    ErrorCode.EXECUTION_RUNNING,
                    f"批次 {batch_id} 的重试已由其它请求接手",
                    {"batch_id": batch_id},
                )
            logger.info("批次 %s 开始第 %d 次尝试", batch_id, retried.attempt)
            self._execute(retried, batch)
            return self.store.get_execution(batch_id)

    def _execute(self, execution: Execution, batch: Batch) -> None:
        """执行并原子落库；中断则置 FAILED，绝不保留半成品。"""
        try:
            findings = run_execution(execution, batch)
        except ExecutionCrashed as exc:
            self.store.fail_execution(batch.batch_id, str(exc))
            logger.warning(
                "执行失败 execution=%s batch=%s attempt=%d reason=%s",
                execution.execution_id, batch.batch_id, execution.attempt, exc,
            )
            return
        committed = self.store.commit_execution(batch.batch_id, findings, len(batch.rows))
        if committed is None:
            # 极端竞态防御：执行权已被其它线程接管，本次结果丢弃
            logger.warning("提交时执行状态已变化，丢弃本次结果 batch=%s", batch.batch_id)
            return
        logger.info(
            "执行结果已原子提交 batch=%s errors=%d warns=%d infos=%d",
            batch.batch_id, committed.error_count, committed.warn_count,
            committed.info_count,
        )

    # ============================================================== #
    # 查询
    # ============================================================== #
    def get_execution(self, batch_id: str) -> Execution:
        execution = self.store.get_execution(batch_id)
        if execution is None:
            raise DQError(
                ErrorCode.EXECUTION_NOT_FOUND,
                f"批次 {batch_id} 尚无执行记录",
                {"batch_id": batch_id},
            )
        return execution

    def conclusion(self, batch_id: str) -> dict:
        if self.store.get_batch(batch_id) is None:
            raise DQError(ErrorCode.BATCH_NOT_FOUND, f"批次 {batch_id} 不存在",
                          {"batch_id": batch_id})
        execution = self.store.get_execution(batch_id)
        if execution is None:
            return {"batch_id": batch_id, "conclusion": "PENDING",
                    "status": None, "error_count": 0, "warn_count": 0,
                    "info_count": 0, "checked_rows": 0, "attempt": 0,
                    "rule_versions": [], "fail_reason": None}
        if execution.status is ExecutionStatus.RUNNING:
            conclusion = "PENDING"
        elif execution.status is ExecutionStatus.FAILED:
            conclusion = "FAILED"
        elif execution.error_count > 0:
            conclusion = "FAIL"
        elif execution.warn_count > 0:
            conclusion = "WARN"
        else:
            conclusion = "PASS"
        return {
            "batch_id": batch_id,
            "conclusion": conclusion,
            "status": execution.status.value,
            "error_count": execution.error_count,
            "warn_count": execution.warn_count,
            "info_count": execution.info_count,
            "checked_rows": execution.checked_rows,
            "attempt": execution.attempt,
            "rule_versions": [
                {"rule_id": s["rule_id"], "version": s["version"]}
                for s in execution.rule_snapshot
            ],
            "fail_reason": execution.fail_reason,
        }

    def list_findings(
        self,
        batch_id: str,
        severity: str | None = None,
        rule_id: str | None = None,
    ) -> list[dict]:
        execution = self.get_execution(batch_id)
        if execution.status is not ExecutionStatus.SUCCEEDED:
            return []  # 未成功完成时没有任何 finding，而不是半成品
        sev = None
        if severity is not None:
            try:
                sev = Severity(severity)
            except ValueError:
                raise DQError(
                    ErrorCode.RULE_PARAM_INVALID,
                    f"severity 过滤值 {severity!r} 非法",
                    {"severity": severity},
                ) from None
        out: list[dict] = []
        for f in execution.findings:  # 已按 (rule_id,row_index,field_name) 排序
            if sev is not None and f.severity is not sev:
                continue
            if rule_id is not None and f.rule_id != rule_id:
                continue
            out.append(self._finding_dict(f))
        return out

    def replay(self, batch_id: str, rule_version: int | None = None) -> dict:
        """按执行时固化的规则快照重新求值（只读），结论须与历史一致。

        rule_version 不为 None 时，只回放该规则某一历史版本（用于对照
        规则变更前后差异）；缺省回放整份快照。
        """
        batch = self.store.get_batch(batch_id)
        if batch is None:
            raise DQError(ErrorCode.BATCH_NOT_FOUND, f"批次 {batch_id} 不存在",
                          {"batch_id": batch_id})
        execution = self.get_execution(batch_id)

        from .engine import _rule_from_snapshot, evaluate_row  # 快照反序列化

        snaps = list(execution.rule_snapshot)
        if rule_version is not None:
            matched = [s for s in snaps if s["version"] == rule_version]
            if not matched:
                raise DQError(
                    ErrorCode.RULE_NOT_FOUND,
                    f"执行快照内没有版本 v{rule_version} 的规则；历史执行只绑定"
                    "当时生效的版本，新规则不影响历史快照",
                    {"batch_id": batch_id, "version": rule_version},
                )
            snaps = matched

        rules = [_rule_from_snapshot(s) for s in snaps]
        rules.sort(key=lambda r: (r.field_name, r.rule_id))
        replayed: list[Finding] = []
        for idx, row in enumerate(batch.rows):
            for rule in rules:
                f = evaluate_row(rule, idx, row)
                if f is not None:
                    replayed.append(f)
        replayed.sort(key=lambda f: (f.rule_id, f.row_index, f.field_name))

        errors = sum(1 for f in replayed if f.severity is Severity.ERROR)
        warns = sum(1 for f in replayed if f.severity is Severity.WARN)
        infos = sum(1 for f in replayed if f.severity is Severity.INFO)
        if errors:
            conclusion = "FAIL"
        elif warns:
            conclusion = "WARN"
        else:
            conclusion = "PASS"
        return {
            "batch_id": batch_id,
            "basis": "execution_rule_snapshot",
            "snapshot_rules": [
                {"rule_id": s["rule_id"], "version": s["version"],
                 "rule_type": s["rule_type"], "severity": s["severity"]}
                for s in snaps
            ],
            "conclusion": conclusion,
            "error_count": errors,
            "warn_count": warns,
            "info_count": infos,
            "findings": [self._finding_dict(f) for f in replayed],
            "matches_historical": (
                rule_version is None
                and execution.status is ExecutionStatus.SUCCEEDED
                and [self._finding_dict(f) for f in replayed]
                == self.list_findings(batch_id)
            ),
        }

    def stats(self) -> Stats:
        return self.store.stats()

    # ============================================================== #
    # 序列化
    # ============================================================== #
    @staticmethod
    def _finding_dict(f: Finding) -> dict:
        return {
            "rule_id": f.rule_id,
            "rule_version": f.rule_version,
            "field_name": f.field_name,
            "rule_type": f.rule_type.value,
            "severity": f.severity.value,
            "row_index": f.row_index,
            "value": f.value,
            "reason": f.reason,
        }

    def execution_dict(self, execution: Execution) -> dict:
        return {
            "execution_id": execution.execution_id,
            "batch_id": execution.batch_id,
            "status": execution.status.value,
            "attempt": execution.attempt,
            "checked_rows": execution.checked_rows,
            "error_count": execution.error_count,
            "warn_count": execution.warn_count,
            "info_count": execution.info_count,
            "fail_reason": execution.fail_reason,
            "created_at": execution.created_at.isoformat(),
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "finished_at": execution.finished_at.isoformat() if execution.finished_at else None,
            "rule_snapshot": execution.rule_snapshot,
            "findings": [self._finding_dict(f) for f in execution.findings],
        }
