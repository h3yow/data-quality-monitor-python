"""校验执行引擎：规则求值、批次执行。

求值是纯函数：给定不可变规则快照与数据行，输出 Finding。
执行中断（crash 钩子）时不产生任何 finding，由服务层转 FAILED 状态。
"""

from __future__ import annotations

import logging
import re
import time

from .models import Batch, Execution, Finding, RuleType, RuleVersion

logger = logging.getLogger("dq.engine")


def _finding(rule: RuleVersion, row_index: int, value: object, reason: str) -> Finding:
    return Finding(
        rule_id=rule.rule_id,
        rule_version=rule.version,
        field_name=rule.field_name,
        rule_type=rule.rule_type,
        severity=rule.severity,
        row_index=row_index,
        value=value,
        reason=reason,
    )


def evaluate_row(rule: RuleVersion, row_index: int, row: dict) -> Finding | None:
    """对单行应用一条规则；命中违规则返回 Finding，否则 None。

    字段值为 None 时：NOT_NULL 报违规；其它取值规则跳过（交给 NOT_NULL
    或 schema 的 nullable 约束处理），避免一条空值产生多条噪音发现。
    """
    value = row.get(rule.field_name)
    rt = rule.rule_type

    if rt is RuleType.NOT_NULL:
        if value is None:
            return _finding(rule, row_index, value, "字段值为 null/缺失")
        return None

    if value is None:
        return None

    if rt is RuleType.RANGE:
        lo = rule.params.get("min")
        hi = rule.params.get("max")
        if lo is not None and value < lo:
            return _finding(rule, row_index, value, f"值 {value} 小于下界 {lo}")
        if hi is not None and value > hi:
            return _finding(rule, row_index, value, f"值 {value} 大于上界 {hi}")
        return None

    if rt is RuleType.ENUM:
        allowed = rule.params["values"]
        if not any(value == a and type(value) is type(a) for a in allowed):
            return _finding(
                rule, row_index, value, f"值 {value!r} 不在允许集合 {allowed} 内"
            )
        return None

    if rt is RuleType.REGEX:
        pattern = rule.params["pattern"]
        if re.fullmatch(pattern, str(value)) is None:
            return _finding(
                rule, row_index, value, f"值 {value!r} 不匹配正则 {pattern!r}"
            )
        return None

    return None


def run_execution(execution: Execution, batch: Batch) -> list[Finding]:
    """对整批执行快照内全部规则，返回 findings（不写库）。

    规则集来自 execution.rule_snapshot 的不可变拷贝，执行期间规则
    变更不影响本次结论。findings 采用确定顺序，保证查询稳定一致。
    """
    # 快照 -> 可求值的轻量规则对象
    rules = [_rule_from_snapshot(s) for s in execution.rule_snapshot]
    rules.sort(key=lambda r: (r.field_name, r.rule_id))

    findings: list[Finding] = []
    logger.info(
        "开始执行 execution=%s batch=%s rows=%d rules=%d attempt=%d",
        execution.execution_id, batch.batch_id, len(batch.rows),
        len(rules), execution.attempt,
    )

    for row_index, row in enumerate(batch.rows):
        # 中断钩子在每行开始时判定：抛错前产生的 findings 一律丢弃，
        # 不写库，不留半完成结果
        crash = crash_simulated(row)
        if crash is not None:
            logger.error(
                "执行中断 execution=%s row=%d reason=%s；本次执行结果全部丢弃",
                execution.execution_id, row_index, crash,
            )
            raise ExecutionCrashed(crash)
        delay = row.get("__delay__")
        if isinstance(delay, (int, float)) and not isinstance(delay, bool) and delay > 0:
            logger.debug("测试延迟钩子 sleep=%ss（用于并发场景）", delay)
            time.sleep(delay)
        for rule in rules:
            f = evaluate_row(rule, row_index, row)
            if f is not None:
                logger.debug(
                    "发现违规 input_row=%d rule=%s(v%d) field=%s value=%r => %s",
                    row_index, rule.rule_id, rule.version, rule.field_name,
                    row.get(rule.field_name), f.reason,
                )
                findings.append(f)

    # 确定性排序：规则、行、字段
    findings.sort(key=lambda f: (f.rule_id, f.row_index, f.field_name))
    logger.info(
        "执行完成 execution=%s findings=%d",
        execution.execution_id, len(findings),
    )
    return findings


def crash_simulated(row: dict) -> str | None:
    """测试钩子：行内带 __crash__ 标记时返回中断原因，模拟执行中断。"""
    marker = row.get("__crash__")
    if marker is None:
        return None
    return str(marker) if marker else "模拟执行中断"


class ExecutionCrashed(RuntimeError):
    """模拟执行中断：服务层捕获后置 FAILED，不保留任何半成品结果。"""


def _rule_from_snapshot(snap: dict) -> RuleVersion:
    from .models import RuleStatus, RuleType, Severity

    return RuleVersion(
        rule_id=snap["rule_id"],
        version=snap["version"],
        schema_name=snap["schema_name"],
        field_name=snap["field_name"],
        rule_type=RuleType(snap["rule_type"]),
        severity=Severity(snap["severity"]),
        params=dict(snap["params"]),
        status=RuleStatus.ACTIVE,
    )
