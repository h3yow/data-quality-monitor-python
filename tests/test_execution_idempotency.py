"""执行、重复提交、中断恢复、严重级别分类与计数稳定性。"""

from __future__ import annotations

import logging

import pytest

from dq_monitor.models import DQError, ErrorCode, ExecutionStatus, Severity
from dq_monitor.service import DataQualityService

logger = logging.getLogger("dq.tests.execution")


def _setup(service: DataQualityService) -> str:
    service.register_schema(
        "orders",
        [{"name": "amount", "dtype": "float"},
         {"name": "status", "dtype": "str"}],
    )
    service.register_rule("amt_rng", "orders", "amount", "RANGE",
                          "ERROR", {"min": 0, "max": 100})
    service.register_rule("amt_nn", "orders", "amount", "NOT_NULL", "ERROR", {})
    service.register_rule("st_enum", "orders", "status", "ENUM",
                          "WARN", {"values": ["PAID", "DONE"]})
    service.register_rule("st_info", "orders", "status", "ENUM",
                          "INFO", {"values": ["PAID"]})
    return "orders"


def test_severity_classification_and_conclusion(service: DataQualityService) -> None:
    _setup(service)
    rows = [
        {"amount": 50.0, "status": "PAID"},   # 干净
        {"amount": 200.0, "status": "PAID"},  # ERROR: 超上界
        {"amount": 10.0, "status": "PEND"},   # WARN + INFO
        {"amount": None, "status": "PAID"},   # ERROR: null（RANGE 跳过不重复报）
    ]
    logger.info("输入批次 rows=%s", rows)
    service.register_batch("b1", "orders", rows)
    execution, accepted = service.submit("b1")
    assert accepted is True
    assert execution.status is ExecutionStatus.SUCCEEDED

    conclusion = service.conclusion("b1")
    logger.info("判定结论=%s 计数=%s", conclusion["conclusion"],
                {k: conclusion[k] for k in
                 ("error_count", "warn_count", "info_count")})
    assert conclusion["conclusion"] == "FAIL"
    assert conclusion["error_count"] == 2  # 超界 + null
    assert conclusion["warn_count"] == 1
    assert conclusion["info_count"] == 1

    # 同一 null 只由 NOT_NULL 报一次（RANGE 跳过）
    null_findings = [
        f for f in service.list_findings("b1")
        if f["row_index"] == 3
    ]
    assert [f["rule_id"] for f in null_findings] == ["amt_nn"]

    # WARN-only 批次结论是 WARN
    service.register_batch("b2", "orders",
                           [{"amount": 1.0, "status": "X"}])
    service.submit("b2")
    assert service.conclusion("b2")["conclusion"] == "WARN"

    # 全干净 -> PASS
    service.register_batch("b3", "orders",
                           [{"amount": 1.0, "status": "PAID"}])
    service.submit("b3")
    assert service.conclusion("b3")["conclusion"] == "PASS"


def test_duplicate_submit_runs_once_and_counts_once(service: DataQualityService) -> None:
    _setup(service)
    service.register_batch("b1", "orders",
                           [{"amount": 200.0, "status": "PAID"}])

    results = [service.submit("b1") for _ in range(5)]
    executions = {id(ex) for ex, _ in results}
    logger.info("5 次重复提交 accepted 序列=%s，执行记录去重后=%s 条",
                [a for _, a in results], len(executions))
    assert [a for _, a in results] == [True, False, False, False, False]
    assert len(executions) == 1

    stats = service.stats()
    assert stats.executions == 1
    assert stats.errors == 1  # 不是 5
    findings = service.list_findings("b1")
    assert len(findings) == 1
    logger.info("判定依据: 仅一次执行 %s，findings=%d，全局 errors=%d",
                results[0][0].execution_id, len(findings), stats.errors)


def test_filtered_findings_are_stable(service: DataQualityService) -> None:
    _setup(service)
    service.register_batch("b1", "orders", [
        {"amount": 200.0, "status": "PAID"},
        {"amount": 10.0, "status": "X"},
    ])
    service.submit("b1")
    warns = service.list_findings("b1", severity="WARN")
    errs = service.list_findings("b1", severity="ERROR")
    by_rule = service.list_findings("b1", rule_id="amt_rng")
    logger.info("过滤输入 severity=WARN -> %s", warns)
    assert {f["severity"] for f in warns} == {"WARN"}
    assert all(f["severity"] == "ERROR" for f in errs)
    assert {f["rule_id"] for f in by_rule} == {"amt_rng"}
    # 重复查询顺序稳定
    again = service.list_findings("b1")
    assert again == service.list_findings("b1")


def test_crash_leaves_recoverable_failed_state(service: DataQualityService) -> None:
    _setup(service)
    service.register_batch("b1", "orders", [
        {"amount": 200.0, "status": "PAID"},
        {"amount": 10.0, "status": "PAID", "__crash__": "worker killed"},
    ])
    execution, accepted = service.submit("b1")
    assert accepted is True
    assert execution.status is ExecutionStatus.FAILED
    assert execution.fail_reason == "worker killed"
    assert execution.findings == []  # 无半成品
    logger.info("中断后状态=%s fail_reason=%s findings=%d（无半完成结果）",
                execution.status, execution.fail_reason, len(execution.findings))

    # 查询不暴露半成品，计数不增加
    assert service.list_findings("b1") == []
    assert service.stats().errors == 0
    assert service.conclusion("b1")["conclusion"] == "FAILED"

    # 修复数据后重试成功（同一执行记录，attempt=2）
    for row in service.store.get_batch("b1").rows:
        row.pop("__crash__", None)
    retried = service.retry("b1")
    assert retried.status is ExecutionStatus.SUCCEEDED
    assert retried.attempt == 2
    assert retried.execution_id == execution.execution_id
    assert service.conclusion("b1")["conclusion"] == "FAIL"  # 200 仍然违规
    assert service.stats().errors == 1
    assert service.stats().executions == 1  # 仍是一条执行记录

    # 终态重试被拒绝
    with pytest.raises(DQError) as ei:
        service.retry("b1")
    assert ei.value.details["reason"] == "terminal_succeeded"


def test_submit_after_failure_retries_automatically(service: DataQualityService) -> None:
    _setup(service)
    service.register_batch("b1", "orders", [
        {"amount": 1.0, "status": "PAID", "__crash__": "boom"},
    ])
    ex1, a1 = service.submit("b1")
    assert ex1.status is ExecutionStatus.FAILED and a1 is True
    for row in service.store.get_batch("b1").rows:
        row.pop("__crash__", None)
    ex2, a2 = service.submit("b1")  # 失败后再次提交 => 自动重试
    assert a2 is True
    assert ex2.status is ExecutionStatus.SUCCEEDED
    assert ex2.attempt == 2


def test_pending_and_missing_states(service: DataQualityService) -> None:
    _setup(service)
    service.register_batch("b0", "orders", [{"amount": 1.0, "status": "PAID"}])
    assert service.conclusion("b0")["conclusion"] == "PENDING"
    with pytest.raises(DQError) as ei:
        service.get_execution("b0")
    assert ei.value.code is ErrorCode.EXECUTION_NOT_FOUND
