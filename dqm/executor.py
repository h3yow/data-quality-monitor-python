"""校验执行器：对批次应用规则快照，产出发现；支持并发去重与中断恢复。

执行语义：
- 执行开始时抓取当前生效规则版本快照，结论只与该快照有关，
  之后规则变更不影响本批次结论（可复现）；
- 同一批次并发/重复提交时，只有抢到执行权的一次真正执行，
  其余调用等待并返回同一份已生效结果（不重复执行、不重复计数）；
- 执行体中任何异常（含测试注入的中断）都会 abort：执行权释放、
  批次状态恢复 REGISTERED，不留下半完成结果，可安全重试。
"""
from __future__ import annotations

import re
import threading
import time
import uuid
from collections.abc import Callable

from dqm.models import Batch, BatchStatus, Execution, Finding, RuleVersion, Severity
from dqm.storage import Storage


def evaluate(rows: list[dict], rules: list[RuleVersion]) -> list[Finding]:
    """纯函数：用给定规则版本对数据求值，返回全部发现（确定性顺序）。"""
    findings: list[Finding] = []
    for rule_version in rules:
        spec = rule_version.spec
        version = rule_version.version

        def hit(row_index: int | None, message: str, value=None) -> None:
            findings.append(Finding(
                rule_id=spec.rule_id, rule_version=version,
                severity=spec.severity, row_index=row_index,
                field=spec.field, message=message, value=value,
            ))

        if spec.rule_type.value == "not_null":
            for i, row in enumerate(rows):
                if spec.field not in row or row[spec.field] is None:
                    hit(i, f"字段 {spec.field!r} 缺失或为 null", row.get(spec.field))
        elif spec.rule_type.value == "range":
            lo, hi = spec.params["min"], spec.params["max"]
            for i, row in enumerate(rows):
                value = row.get(spec.field)
                if value is None:
                    continue  # 缺失由 NOT_NULL 规则负责，避免重复计数
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    hit(i, f"字段 {spec.field!r} 非数值: {value!r}", value)
                elif not lo <= value <= hi:
                    hit(i, f"字段 {spec.field!r}={value} 超出 [{lo}, {hi}]", value)
        elif spec.rule_type.value == "regex":
            pattern = re.compile(spec.params["pattern"])
            for i, row in enumerate(rows):
                value = row.get(spec.field)
                if value is None:
                    continue
                if not isinstance(value, str) or not pattern.fullmatch(value):
                    hit(i, f"字段 {spec.field!r}={value!r} 不匹配 /{pattern.pattern}/", value)
        elif spec.rule_type.value == "enum":
            allowed = set(spec.params["allowed"])
            for i, row in enumerate(rows):
                value = row.get(spec.field)
                if value is None:
                    continue
                if value not in allowed:
                    hit(i, f"字段 {spec.field!r}={value!r} 不在允许集合内", value)
        elif spec.rule_type.value == "unique":
            seen: dict[object, int] = {}
            for i, row in enumerate(rows):
                value = row.get(spec.field)
                if value is None:
                    continue
                if value in seen:
                    hit(i, f"字段 {spec.field!r} 值 {value!r} 与第 {seen[value]} 行重复", value)
                else:
                    seen[value] = i
    return findings


def _final_status(findings: list[Finding]) -> BatchStatus:
    return (BatchStatus.FAILED if any(f.severity is Severity.FAILURE for f in findings)
            else BatchStatus.SUCCEEDED)


def run_batch(storage: Storage, batch: Batch,
              interrupt: Callable[[], None] | None = None) -> Execution:
    """执行批次校验，返回生效的 Execution（可能是本调用或并发调用产生的）。"""
    snapshot = storage.current_rules()
    execution = Execution(
        execution_id=uuid.uuid4().hex,
        batch_id=batch.batch_id,
        attempt=1,
        rule_versions={rv.spec.rule_id: rv.version for rv in snapshot},
    )
    acquired, effective_id = storage.try_begin_execution(execution)
    if not acquired:
        # 并发/重复提交：去重，等待生效执行完成并返回同一份结果
        return _wait_committed(storage, batch.batch_id, effective_id)
    try:
        if interrupt is not None:
            interrupt()  # 测试注入的中断点：模拟执行中途崩溃
        findings = evaluate(batch.rows, snapshot)
        storage.commit_execution(execution.execution_id, findings, _final_status(findings))
    except BaseException:
        storage.abort_execution(execution.execution_id)
        raise
    return execution


def _wait_committed(storage: Storage, batch_id: str, execution_id: str | None,
                    timeout: float = 30.0) -> Execution:
    """等待生效执行提交完成；若其被 abort（中断恢复），由调用方重试。"""
    condition = threading.Condition()
    deadline = time.monotonic() + timeout
    while True:
        execution = storage.committed_execution(batch_id)
        if execution is None:
            raise RuntimeError(f"批次 {batch_id} 的生效执行被中断，需重试")
        if execution.status == "committed":
            return execution
        with condition:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"等待批次 {batch_id} 执行提交超时")
            condition.wait(timeout=min(0.01, remaining))
