"""并发提交：同批次多线程同时提交，只有一次有效执行、一份结果、一次计数。"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from dq_monitor.models import ExecutionStatus
from dq_monitor.service import DataQualityService

logger = logging.getLogger("dq.tests.concurrency")


def _setup(service: DataQualityService) -> None:
    service.register_schema("orders", [{"name": "amount", "dtype": "float"}])
    service.register_rule("r", "orders", "amount", "RANGE",
                          "ERROR", {"min": 0, "max": 100})


def test_concurrent_submit_single_effective_execution(
    service: DataQualityService,
) -> None:
    _setup(service)
    # 首行延迟放大竞态窗口；2 行中有 3 个违规
    service.register_batch("b1", "orders", [
        {"amount": 200.0, "__delay__": 0.1},
        {"amount": 300.0},
    ])

    n = 12
    barrier = threading.Barrier(n)

    def submit() -> tuple[bool, str, str, int]:
        barrier.wait()  # 尽量让所有线程同时进入
        ex, accepted = service.submit("b1")
        return accepted, ex.execution_id, ex.status.value, ex.error_count

    with ThreadPoolExecutor(max_workers=n) as pool:
        outcomes = list(pool.map(lambda _: submit(), range(n)))

    accepted = [o[0] for o in outcomes]
    exec_ids = {o[1] for o in outcomes}
    statuses = {o[2] for o in outcomes}
    counts = {o[3] for o in outcomes}

    logger.info(
        "并发输入: %d 线程同时提交 b1；accepted=%s", n, accepted
    )
    logger.info(
        "判定依据: 不同 execution_id 数=%d（期望1），状态集合=%s，"
        "各线程看到的 error_count=%s",
        len(exec_ids), statuses, counts,
    )
    assert accepted.count(True) == 1
    assert len(exec_ids) == 1
    assert statuses == {"SUCCEEDED"}
    assert counts == {2}  # 200、300 两行均超界

    stats = service.stats()
    assert stats.executions == 1
    assert stats.errors == 2
    assert len(service.list_findings("b1")) == 2


def test_concurrent_submit_while_running_returns_same_record(
    service: DataQualityService,
) -> None:
    _setup(service)
    service.register_batch("b2", "orders", [
        {"amount": 1.0, "__delay__": 0.3},
        {"amount": 2.0},
    ])

    results: list = []

    def first() -> None:
        ex, accepted = service.submit("b2")
        results.append(("first", accepted, ex.status.value, ex.execution_id))

    def later() -> None:
        ex, accepted = service.submit("b2")
        results.append(("later", accepted, ex.status.value, ex.execution_id))

    t1 = threading.Thread(target=first)
    t1.start()
    # 确保 t1 已进入 RUNNING
    import time
    time.sleep(0.05)
    threads = [threading.Thread(target=later) for _ in range(5)]
    for t in threads:
        t.start()
    t1.join()
    for t in threads:
        t.join()

    labels = [r[0] for r in results]
    later_results = [r for r in results if r[0] == "later"]
    logger.info("执行中并发提交输入: 1 个执行线程 + 5 个并发提交线程")
    logger.info("判定依据: later accepted=%s 状态=%s",
                [r[1] for r in later_results], [r[2] for r in later_results])
    assert labels.count("first") == 1
    assert all(r[1] is False for r in later_results)
    assert all(r[2] == "SUCCEEDED" for r in later_results)
    ids = {r[3] for r in results}
    assert len(ids) == 1
    assert service.stats().executions == 1


def test_concurrent_retry_of_failed_execution(service: DataQualityService) -> None:
    from dq_monitor.models import DQError, ErrorCode

    _setup(service)
    service.register_batch("b3", "orders", [
        {"amount": 999.0, "__crash__": "boom"},
    ])
    ex, _ = service.submit("b3")
    assert ex.status is ExecutionStatus.FAILED
    for row in service.store.get_batch("b3").rows:
        row.pop("__crash__", None)

    n = 8
    barrier = threading.Barrier(n)
    outcomes: list = []

    def retry_once() -> None:
        barrier.wait()
        try:
            r = service.retry("b3")
            outcomes.append(("ok", r.attempt, r.error_count, r.status.value))
        except DQError as e:
            outcomes.append(("err", e.code.value, e.details.get("reason"), None))

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(lambda _: retry_once(), range(n)))

    logger.info("并发重试输入: %d 线程同时 retry FAILED 批次", n)
    ok = [o for o in outcomes if o[0] == "ok"]
    terminal = [o for o in outcomes if o[0] == "err"]
    logger.info("判定依据: 有效重试=%s，其余命中终态冲突=%s", ok, terminal)
    assert len(ok) == 1
    # 胜者在批次锁内完成执行后，其余线程读到 SUCCEEDED 终态 -> terminal_succeeded
    assert all(o[1] == ErrorCode.RULE_CONFLICT.value for o in terminal)
    assert all(o[2] == "terminal_succeeded" for o in terminal)
    assert ok[0][2] == 1  # 999 超界 -> 1 个 error
    assert service.stats().executions == 1
    assert service.stats().errors == 1
