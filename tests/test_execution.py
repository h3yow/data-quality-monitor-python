"""执行层：重复提交去重、并发执行唯一生效、中断后状态可恢复。"""
import logging
import threading

import pytest

from dqm.executor import run_batch
from dqm.service import DQService

logger = logging.getLogger("test.execution")

ROWS = [
    {"id": 1, "name": "alice", "age": 30},
    {"id": 1, "name": None, "age": 200},   # 命中 unique + not_null + range
    {"id": 3, "age": 40},                  # 命中 not_null
]


def make_service() -> DQService:
    service = DQService()
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    service.register_rule({"rule_id": "uq", "rule_type": "unique", "field": "id",
                           "severity": "warning"})
    return service


def log_report(tag: str, report) -> None:
    logger.info("[%s] 输入数据: %s", tag, ROWS)
    logger.info("[%s] 判定依据: rule_versions=%s, status=%s",
                tag, report.rule_versions, report.status.value)
    for f in report.failures + report.warnings:
        logger.info("[%s] 发现: rule=%s v%s severity=%s row=%s 依据: %s",
                    tag, f.rule_id, f.rule_version, f.severity.value,
                    f.row_index, f.message)


def test_duplicate_submission_deduplicated() -> None:
    """同一批次重复登记 + 重复触发校验：只生效一次，结论与计数不重复。"""
    service = make_service()
    assert service.register_batch("b1", ROWS) is True
    assert service.register_batch("b1", ROWS) is False  # 重复登记被幂等拒绝

    first = service.validate_batch("b1")
    second = service.validate_batch("b1")  # 重复提交
    log_report("重复提交", second)

    assert second.execution_id == first.execution_id
    assert second.counts == {"failures": 3, "warnings": 1}
    assert second.status.value == "failed"
    # 存储中该批次只有一个生效执行
    assert service.storage.committed_execution("b1").execution_id == first.execution_id


def test_concurrent_validation_single_effective_execution() -> None:
    """并发提交同一批次：仅一次执行生效，所有线程得到一致结论。"""
    service = make_service()
    service.register_batch("bc", ROWS)
    barrier = threading.Barrier(8)
    reports, errors = [], []

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            reports.append(service.validate_batch("bc"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, f"并发执行出现错误: {errors}"
    assert len(reports) == 8
    execution_ids = {r.execution_id for r in reports}
    logger.info("并发 8 线程, 生效 execution 数=%d, 结论分布=%s",
                len(execution_ids), [r.counts for r in reports])
    assert len(execution_ids) == 1, "并发下只允许一次有效执行"
    assert all(r.counts == reports[0].counts for r in reports)
    log_report("并发", reports[0])


def test_interrupted_execution_recovers_and_retries() -> None:
    """执行中断：状态恢复 REGISTERED、无半完成结果，重试后正常完成。"""
    service = make_service()
    service.register_batch("bx", ROWS)
    batch = service.storage.get_batch("bx")

    def crash() -> None:
        raise RuntimeError("模拟执行中断")

    with pytest.raises(RuntimeError):
        run_batch(service.storage, batch, interrupt=crash)

    # 中断后：状态明确可恢复，无半完成结果
    assert service.batch_status("bx").value == "registered"
    assert service.storage.committed_execution("bx") is None
    report = service.get_report("bx")
    assert report.execution_id is None and report.counts == {"failures": 0, "warnings": 0}

    # 重试成功，结论与正常执行一致
    retried = service.validate_batch("bx")
    log_report("中断重试", retried)
    assert retried.status.value == "failed"
    assert retried.counts == {"failures": 3, "warnings": 1}


def test_concurrent_validation_with_one_crash() -> None:
    """并发提交中一个执行中断：其余提交仍收敛到唯一生效执行。"""
    service = make_service()
    service.register_batch("by", ROWS)
    batch = service.storage.get_batch("by")
    barrier = threading.Barrier(2)
    results, errors = [], []

    def crasher() -> None:
        barrier.wait(timeout=5)
        try:
            run_batch(service.storage, batch,
                      interrupt=lambda: (_ for _ in ()).throw(RuntimeError("crash")))
        except RuntimeError:
            pass

    def normal() -> None:
        barrier.wait(timeout=5)
        try:
            results.append(service.validate_batch("by"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=crasher), threading.Thread(target=normal)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    # 若正常线程恰好等到被 abort 的执行，则以重试补齐
    if not results:
        results.append(service.validate_batch("by"))
    final = service.validate_batch("by")
    logger.info("崩溃并发后最终结论: status=%s counts=%s", final.status.value, final.counts)
    assert final.status.value == "failed"
    assert final.counts == {"failures": 3, "warnings": 1}
    assert not errors
