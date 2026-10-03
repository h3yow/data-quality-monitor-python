"""校验执行器：对批次应用规则快照，产出发现；支持并发去重与中断恢复。

执行语义：
- 执行开始时抓取当前生效规则版本快照，结论只与该快照有关，
  之后规则变更不影响本批次结论（可复现）；
- 同一批次并发/重复提交时，只有抢到执行权的一次真正执行，
  其余调用等待并返回同一份已生效结果（不重复执行、不重复计数）；
- 等待方自愈：若抢到执行权的一方中途被 abort（中断/异常退出），
  等待方不会把内部错误抛给调用方，而是重新竞争执行权、自己补跑，
  最终收敛出唯一一份生效结论；
- 执行体中任何异常（含测试注入的中断）都会 abort：执行权释放、
  批次状态恢复 REGISTERED，不留下半完成结果，可安全重试。
"""
from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable

from dqm.models import Batch, BatchStatus, Execution, Finding, RuleVersion, Severity
from dqm.storage import Storage

# 单批次处理上界：行数过大时明确拒绝（由上层转译为 WorkloadTooLarge），
# 避免单个超大批次把求值变成无界操作；该上界内结论与逐行处理完全一致。
MAX_ROWS_PER_BATCH = 100_000

# 等待占用方（并发执行/回溯租约）尘埃落定的最大重试轮数，防止异常竞态死循环
_WAIT_SPINS = 6000


class BatchTooLarge(Exception):
    """单个批次行数超过处理上界。"""


def evaluate(rows: list[dict], rules: list[RuleVersion]) -> list[Finding]:
    """纯函数：用给定规则版本对数据求值，返回全部发现（确定性顺序）。"""
    if len(rows) > MAX_ROWS_PER_BATCH:
        raise BatchTooLarge(
            f"批次行数 {len(rows)} 超过单批上界 {MAX_ROWS_PER_BATCH}")
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


# 公开别名：试算/回溯等模块复用同一份"是否失败"判定，保证结论口径一致
final_status = _final_status


def run_batch(storage: Storage, batch: Batch,
              interrupt: Callable[[], None] | None = None) -> Execution:
    """执行批次校验，返回生效的 Execution（可能是本调用或并发调用产生的）。

    定心循环：没抢到执行权就等待占用方尘埃落定——占用方提交则复用其结论；
    占用方被中断（abort）则回到循环顶部重新竞争，由本调用自己补跑，
    保证等待方也能收敛出唯一一份生效结论，不把内部中断抛给调用方。
    """
    for _ in range(_WAIT_SPINS):
        snapshot = storage.current_rules()
        execution = Execution(
            execution_id=uuid.uuid4().hex,
            batch_id=batch.batch_id,
            attempt=1,
            rule_versions={rv.spec.rule_id: rv.version for rv in snapshot},
        )
        acquired, _effective_id = storage.try_begin_execution(execution)
        if not acquired:
            # 并发/重复提交，或回溯租约占用：等待其完成并复用同一份结果——
            # 正常触发与回溯撞车时，两边拿到的结论对得上；
            # 若占用方中断退出（返回 None），回到循环顶部自己补跑。
            outcome = wait_for_effective_execution(storage, batch.batch_id)
            if outcome is not None:
                return outcome
            continue
        try:
            if interrupt is not None:
                interrupt()  # 测试注入的中断点：模拟执行中途崩溃
            findings = evaluate(batch.rows, snapshot)
            storage.commit_execution(execution.execution_id, findings,
                                     _final_status(findings))
        except BatchTooLarge:
            storage.abort_execution(execution.execution_id)
            from dqm.errors import WorkloadTooLarge
            raise WorkloadTooLarge(
                f"批次 {batch.batch_id!r} 行数 {len(batch.rows)} 超过单批上界 "
                f"{MAX_ROWS_PER_BATCH}，拒绝执行（不留半完成结果）")
        except BaseException:
            storage.abort_execution(execution.execution_id)
            raise
        return execution
    raise TimeoutError(f"批次 {batch.batch_id} 等待占用方释放超时")


def wait_for_effective_execution(storage: Storage, batch_id: str,
                                 timeout: float = 30.0) -> Execution | None:
    """等待批次当前在途执行（正常校验或回溯）尘埃落定。

    返回已提交的生效 Execution；若在途占用方被 abort 退出且批次没有任何
    生效结论（典型：从未校验过的批次上执行被中断），返回 None——
    调用方应重新竞争执行权、自己补跑，而不是把中断抛给最终用户。
    """
    deadline = time.monotonic() + timeout
    while True:
        owner = storage.rerun_owner(batch_id)
        if owner is not None:
            execution = storage.get_execution(owner)
            if execution is not None and execution.status == "committed":
                return execution
        else:
            execution = storage.committed_execution(batch_id)
            if execution is None:
                return None  # 占用方已中断退出，批次空窗：调用方补跑
            if execution.status == "committed":
                return execution
        if time.monotonic() >= deadline:
            raise TimeoutError(f"等待批次 {batch_id} 的执行提交超时")
        time.sleep(0.005)
