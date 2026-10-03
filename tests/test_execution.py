"""执行层：重复提交去重、并发执行唯一生效、中断后状态可恢复。"""
import logging
import threading

import pytest

from dqm.executor import run_batch
from dqm.models import BatchStatus
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
    """并发提交中一个执行中断：等待方自行补跑收敛，无需调用方手动重试。"""
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
    # 等待方必须自己收敛出结论：不抛内部错误、不需要调用方再点一次
    assert not errors, f"等待方不应收到内部错误: {errors}"
    assert len(results) == 1, "等待方应当场拿到结论，而非靠事后手动重试"
    final = service.validate_batch("by")
    logger.info("崩溃并发后最终结论: status=%s counts=%s", final.status.value, final.counts)
    assert final.status.value == "failed"
    assert final.counts == {"failures": 3, "warnings": 1}
    assert final.execution_id == results[0].execution_id


def test_waiting_duplicate_self_converges_after_owner_interrupted() -> None:
    """抢到执行权的一次被中断：正在等待的重复调用自己补跑，收敛出唯一生效结论。"""
    service = make_service()
    service.register_batch("bz", ROWS)
    batch = service.storage.get_batch("bz")
    barrier = threading.Barrier(2)
    calls = {"n": 0}
    results, errors = [], []

    def interrupt_once() -> None:
        # 只有第一个进入执行体（即抢到执行权）的调用被中断；
        # 先挂起片刻，确保另一个线程已进入等待路径，精确复现
        # "执行方中断时等待方正在等"的撞车窗口
        calls["n"] += 1
        if calls["n"] == 1:
            import time as _time
            _time.sleep(0.15)
            raise RuntimeError("模拟执行中断")

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(run_batch(service.storage, batch,
                                     interrupt=interrupt_once))
        except RuntimeError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    # 恰好一个线程（被中断的执行方）抛出，另一个（等待方）自行补跑成功
    assert len(errors) == 1 and "模拟执行中断" in str(errors[0])
    assert len(results) == 1
    execution = results[0]
    assert execution.status == "committed"
    # 批次最终只有一条生效结论，状态不停在 registered/running
    committed = service.storage.committed_execution("bz")
    assert committed is not None and committed.execution_id == execution.execution_id
    assert service.batch_status("bz") is BatchStatus.FAILED
    assert len(service.storage.revisions("bz")) == 1
    # 事后再触发校验：去重返回同一份结论
    report = service.validate_batch("bz")
    assert report.execution_id == execution.execution_id
    assert report.counts == {"failures": 3, "warnings": 1}
    logger.info("等待方自愈：中断后由等待方补跑提交 execution=%s，结论唯一",
                execution.execution_id[:8])
