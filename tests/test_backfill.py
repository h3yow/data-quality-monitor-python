"""历史批次回溯：结论版本化对照、并发唯一生效、中断重试、失败隔离、有界一致性。"""
import logging
import threading

import pytest

from dqm import backfill as bm
from dqm.backfill import backfill_batch, backfill_batches
from dqm.errors import BackfillError
from dqm.executor import evaluate, run_batch
from dqm.models import BackfillItemStatus, BatchStatus, Severity
from dqm.service import DQService

logger = logging.getLogger("test.backfill")

ROWS = [{"age": 150}, {"age": 300}]  # 旧 [0,200]：1 失败；新 [0,120]：2 失败


def make_service(n_batches: int = 3) -> DQService:
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 200}})
    for i in range(n_batches):
        bid = f"b{i:03d}"
        service.register_batch(bid, ROWS)
        service.validate_batch(bid)
    # 规则收紧后再回溯
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    return service


def test_backfill_appends_revision_and_keeps_old() -> None:
    """回溯后新旧结论并排可查：翻盘、新增/消失命中正确，旧结论不被覆盖。"""
    service = make_service(n_batches=1)
    old_report = service.get_report("b000")
    assert old_report.status is BatchStatus.FAILED and old_report.revision_no == 1

    comparison = service.backfill_one("b000")
    logger.info("回溯对照: rev%s(%s) -> rev%s(%s) 翻盘=%s 新增=%d 消失=%d",
                comparison.previous_revision_no,
                comparison.previous_status.value,
                comparison.current_revision_no, comparison.current_status.value,
                comparison.flipped, len(comparison.added), len(comparison.removed))
    assert comparison.reran is True
    assert comparison.previous_revision_no == 1 and comparison.current_revision_no == 2
    assert comparison.flipped is False  # failed -> failed，不算翻盘
    # 150 旧通过/新失败（新增），300 旧失败命中的版本与依据变化（旧消失+新新增）
    assert len(comparison.added) == 2 and len(comparison.removed) == 1

    revisions = service.list_revisions("b000")
    assert len(revisions) == 2
    v1, v2 = revisions
    assert v1.source == "normal" and v2.source == "backfill"
    assert dict(v1.rule_versions) == {"rg": 1} and dict(v2.rule_versions) == {"rg": 2}
    assert sum(1 for f in v1.findings if f.severity is Severity.FAILURE) == 1
    assert sum(1 for f in v2.findings if f.severity is Severity.FAILURE) == 2

    # 当前报告指向新版本，但旧结论仍可按版本查到
    latest_report = service.get_report("b000")
    assert latest_report.revision_no == 2 and latest_report.rule_versions == {"rg": 2}
    old_snapshot = [service.storage.get_rule_version("rg", 1)]
    assert evaluate(ROWS, old_snapshot) == list(v1.findings)
    logger.info("旧结论 v1 仍可复现: %s", [f.message for f in v1.findings])


def test_backfill_flip_succeeded_to_failed() -> None:
    """全部通过的批次在规则收紧后回溯：结论翻盘。"""
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 200}})
    service.register_batch("ok", [{"age": 150}])
    assert service.validate_batch("ok").status is BatchStatus.SUCCEEDED
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    cmp_res = service.backfill_one("ok")
    assert cmp_res.flipped is True
    assert cmp_res.previous_status is BatchStatus.SUCCEEDED
    assert cmp_res.current_status is BatchStatus.FAILED
    logger.info("翻盘对照: %s -> %s", cmp_res.previous_status.value,
                cmp_res.current_status.value)


def test_backfill_deduplicated_when_snapshot_current() -> None:
    """规则快照未变时回溯：去重不重跑，不产生新版本、不重复计数。"""
    service = make_service(n_batches=1)
    service.backfill_one("b000")  # 第一次真正重跑到 v2
    rev, reran = backfill_batch(service.storage, service.storage.get_batch("b000"))
    assert reran is False and rev.revision_no == 2
    assert len(service.storage.revisions("b000")) == 2
    logger.info("快照已最新时回溯被去重（reran=False），结论版本仍为 2")


def test_backfill_job_ledger_distinguishes_rerun_and_dedup() -> None:
    """回溯作业账本：真重跑与去重条目如实区分，不混成一种记录。"""
    service = make_service(n_batches=2)  # b000/b001 均按 v1 定论，规则已收紧到 v2
    service.backfill_one("b000")  # b000 先单独回溯到 v2（真重跑）

    # 同一作业内：b000 快照已最新（应记去重），b001 落后一版（应记真重跑）
    job = service.backfill_batches(["b000", "b001"], max_workers=2)
    dedup_item, rerun_item = job.items["b000"], job.items["b001"]
    logger.info("混合作业账本: b000 reran=%s rev=%s, b001 reran=%s rev=%s",
                dedup_item.reran, dedup_item.revision_no,
                rerun_item.reran, rerun_item.revision_no)
    assert dedup_item.status is BackfillItemStatus.SUCCEEDED
    assert rerun_item.status is BackfillItemStatus.SUCCEEDED
    assert dedup_item.reran is False and dedup_item.revision_no == 2
    assert rerun_item.reran is True and rerun_item.revision_no == 2
    # 去重条目没有追加版本；真重跑条目恰好追加一版
    assert [r.revision_no for r in service.storage.revisions("b000")] == [1, 2]
    assert [r.revision_no for r in service.storage.revisions("b001")] == [1, 2]

    # 规则未再变化时整体回溯：作业里每条记录都应是去重，且与真重跑记录可区分
    job2 = service.backfill_batches(["b000", "b001"], max_workers=2)
    assert all(item.reran is False for item in job2.items.values())
    assert all(item.revision_no == 2 for item in job2.items.values())
    assert all(len(service.storage.revisions(bid)) == 2 for bid in ("b000", "b001"))
    logger.info("规则未变的整体回溯：全部条目 reran=False（纯去重），版本数不变")


def test_concurrent_backfill_single_effective() -> None:
    """同一批次并发回溯：只有一次真正执行，结论版本不翻倍。"""
    service = make_service(n_batches=1)
    batch = service.storage.get_batch("b000")
    barrier = threading.Barrier(6)
    results, errors = [], []

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(backfill_batch(service.storage, batch))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, errors
    assert len(results) == 6
    reran_count = sum(1 for _, reran in results if reran)
    assert reran_count == 1, f"只应一次真正重跑，实际 {reran_count}"
    revisions = service.storage.revisions("b000")
    assert [r.revision_no for r in revisions] == [1, 2]
    logger.info("6 线程并发回溯：真正重跑 %d 次，结论版本=%s",
                reran_count, [r.revision_no for r in revisions])


def test_backfill_interrupt_recovers_without_partial_result() -> None:
    """回溯中途崩溃：状态恢复旧结论、无新版本残留，重试成功。"""
    service = make_service(n_batches=1)
    batch = service.storage.get_batch("b000")

    with pytest.raises(RuntimeError):
        backfill_batch(service.storage, batch,
                       interrupt=lambda: (_ for _ in ()).throw(RuntimeError("回溯中断")))

    assert service.batch_status("b000") is BatchStatus.FAILED  # 恢复为旧结论状态
    revisions = service.storage.revisions("b000")
    assert [r.revision_no for r in revisions] == [1]  # 无半完成版本
    assert service.storage.rerun_owner("b000") is None
    report = service.get_report("b000")
    assert report.revision_no == 1 and report.rule_versions == {"rg": 1}
    logger.info("回溯中断后仍为旧结论 v1，无半完成结果，可重试")

    cmp_res = service.backfill_one("b000")  # 单独重试到成功
    assert cmp_res.current_revision_no == 2
    assert service.get_report("b000").revision_no == 2
    logger.info("中断后单独重试成功，结论升级到 v2")


def test_bulk_backfill_failure_isolation_and_job_retry() -> None:
    """批量回溯中某批次失败被隔离记账，可按作业重试到成功。"""
    service = make_service(n_batches=3)
    original = bm.backfill_batch

    call = {"n": 0}

    def flaky(storage, batch, interrupt=None):
        # 让 b001 的第一次回溯失败
        if batch.batch_id == "b001" and call["n"] == 0:
            call["n"] += 1
            raise RuntimeError("模拟某批次中途失败")
        return original(storage, batch, interrupt=interrupt)

    bm.backfill_batch = flaky  # type: ignore[misc]
    try:
        job = service.backfill_batches(["b000", "b001", "b002"], max_workers=3)
    finally:
        bm.backfill_batch = original  # type: ignore[misc]

    items = job.items
    assert items["b000"].status is BackfillItemStatus.SUCCEEDED
    assert items["b002"].status is BackfillItemStatus.SUCCEEDED
    assert items["b001"].status is BackfillItemStatus.FAILED
    assert "模拟某批次中途失败" in (items["b001"].error or "")
    logger.info("批量回溯失败隔离: %s",
                {k: v.status.value for k, v in items.items()})
    # 失败批次不连累其它批次，且自身无半完成结论
    assert service.get_report("b000").revision_no == 2
    assert service.get_report("b001").revision_no == 1
    assert job.finished_at is None  # 尚有失败，作业不算完成

    retried = service.retry_backfill_job(job.job_id)
    assert all(i.status is BackfillItemStatus.SUCCEEDED
               for i in retried.items.values())
    assert retried.items["b000"].revision_no == 2  # 已成功条目不重跑
    assert retried.items["b001"].revision_no == 2  # 失败批次重试成功
    assert retried.finished_at is not None
    logger.info("作业重试后全部成功: %s",
                {k: v.revision_no for k, v in retried.items.items()})


def test_backfill_missing_batches_rejected() -> None:
    """回溯不存在的批次：整单明确拒绝。"""
    service = make_service(n_batches=1)
    with pytest.raises(BackfillError):
        service.backfill_batches(["b000", "nope"])
    with pytest.raises(BackfillError):
        service.backfill_batches([])
    logger.info("不存在/空批次列表的回溯请求被明确拒绝")


def test_backfill_batch_count_bound() -> None:
    """单次回溯超过批次数上界：拒绝而非截断。"""
    service = make_service(n_batches=1)
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(bm, "MAX_BACKFILL_BATCHES", 2)
        with pytest.raises(BackfillError):
            service.backfill_batches(["a", "b", "c"])
    logger.info("超过回溯批次数上界被拒绝")


def test_backfill_oversized_batch_rejected(monkeypatch) -> None:
    """单个超大批次回溯：明确拒绝，不产生新版本、租约被释放。"""
    from dqm.errors import WorkloadTooLarge

    service = DQService()
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    service.register_batch("big", [{"name": "a"}, {"name": "b"}, {}])
    service.validate_batch("big")
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name",
                           "severity": "warning"})
    monkeypatch.setattr(bm, "MAX_ROWS_PER_BATCH", 2)
    with pytest.raises(WorkloadTooLarge):
        service.backfill_one("big")
    assert service.storage.rerun_owner("big") is None
    assert len(service.storage.revisions("big")) == 1  # 旧结论保留，无半完成版本
    # 批量预检同样拒绝
    with pytest.raises(WorkloadTooLarge):
        service.backfill_batches(["big"])
    logger.info("超大单批回溯被拒绝：租约释放、旧结论保留")


def test_backfill_collides_with_normal_validation() -> None:
    """回溯与正常触发校验撞车：两边最终拿到同一份最新结论。"""
    service = make_service(n_batches=1)
    batch = service.storage.get_batch("b000")
    entered = threading.Event()
    proceed = threading.Event()
    results = {}

    def backfiller() -> None:
        # 在求值环节（锁外）挂起，精确制造"回溯已占租约、正在执行中"的窗口
        original_evaluate = bm.evaluate

        def slow_evaluate(rows, snapshot):
            entered.set()
            proceed.wait(timeout=5)
            return original_evaluate(rows, snapshot)

        bm.evaluate = slow_evaluate  # type: ignore[misc]
        try:
            results["backfill"] = backfill_batch(service.storage, batch)
        finally:
            bm.evaluate = original_evaluate  # type: ignore[misc]

    def validator() -> None:
        entered.wait(timeout=5)
        # 回溯已占租约正在执行；正常校验应等待并复用回溯结论
        results["validate"] = service.validate_batch("b000")

    t1 = threading.Thread(target=backfiller)
    t2 = threading.Thread(target=validator)
    t1.start()
    entered.wait(timeout=5)
    t2.start()
    import time as _time
    _time.sleep(0.05)  # 确保正常校验已进入等待
    proceed.set()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert "backfill" in results and "validate" in results
    revision, _reran = results["backfill"]
    assert revision.revision_no == 2
    assert results["validate"].revision_no == 2
    assert results["validate"].rule_versions == {"rg": 2}
    assert [r.revision_no for r in service.storage.revisions("b000")] == [1, 2]
    logger.info("回溯与正常校验撞车后结论一致：均为 v2")


def test_race_on_unvalidated_batch_backfill_first() -> None:
    """未校验批次上回溯先占租约、正常校验同时到达：只留一条生效结论。"""
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    service.register_batch("u1", [{"age": 150}, {"age": 300}])  # 只登记，未校验
    batch = service.storage.get_batch("u1")
    entered = threading.Event()
    proceed = threading.Event()
    results, errors = {}, []

    def backfiller() -> None:
        original_evaluate = bm.evaluate

        def slow_evaluate(rows, snapshot):
            entered.set()                    # 回溯已占租约、正在求值
            proceed.wait(timeout=5)
            return original_evaluate(rows, snapshot)

        bm.evaluate = slow_evaluate  # type: ignore[misc]
        try:
            results["backfill"] = backfill_batch(service.storage, batch)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            bm.evaluate = original_evaluate  # type: ignore[misc]

    def validator() -> None:
        entered.wait(timeout=5)
        try:
            results["validate"] = service.validate_batch("u1")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=backfiller)
    t1.start()
    entered.wait(timeout=5)
    t2 = threading.Thread(target=validator)
    t2.start()
    import time as _time
    _time.sleep(0.05)  # 确保正常校验已进来并进入等待
    proceed.set()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert not errors, errors
    revision, reran = results["backfill"]
    report = results["validate"]
    logger.info("未校验批次撞车（回溯先）: backfill rev=%s reran=%s, "
                "validate rev=%s execution=%s",
                revision.revision_no, reran, report.revision_no, report.execution_id)
    assert reran is True and revision.revision_no == 1
    # 正常校验拿到的是回溯产出的同一份结论，而不是自己再跑一份
    assert report.revision_no == 1 and report.execution_id == revision.execution_id
    revisions = service.storage.revisions("u1")
    assert len(revisions) == 1 and revisions[0].source == "backfill"
    assert service.storage.committed_execution("u1").execution_id == revision.execution_id


def test_race_on_unvalidated_batch_validate_first() -> None:
    """未校验批次上正常校验先占执行权、回溯同时到达：回溯去重，仍只有一条结论。"""
    import dqm.executor as ex

    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    service.register_batch("u2", [{"age": 150}, {"age": 300}])  # 只登记，未校验
    batch = service.storage.get_batch("u2")
    entered = threading.Event()
    proceed = threading.Event()
    results, errors = {}, []

    original_evaluate = ex.evaluate

    def slow_evaluate(rows, snapshot):
        entered.set()                        # 正常校验已占执行权、正在求值
        proceed.wait(timeout=5)
        return original_evaluate(rows, snapshot)

    def validator() -> None:
        ex.evaluate = slow_evaluate  # type: ignore[misc]
        try:
            results["validate"] = service.validate_batch("u2")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            ex.evaluate = original_evaluate  # type: ignore[misc]

    def backfiller() -> None:
        entered.wait(timeout=5)
        try:
            results["backfill"] = backfill_batch(service.storage, batch)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=validator)
    t1.start()
    entered.wait(timeout=5)
    t2 = threading.Thread(target=backfiller)
    t2.start()
    import time as _time
    _time.sleep(0.05)  # 确保回溯已进来并进入等待
    proceed.set()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert not errors, errors
    report = results["validate"]
    revision, reran = results["backfill"]
    logger.info("未校验批次撞车（校验先）: validate rev=%s, backfill rev=%s reran=%s",
                report.revision_no, revision.revision_no, reran)
    # 快照已与正常校验结论一致：回溯被去重，不产生第二条生效结论
    assert reran is False and revision.revision_no == 1
    assert report.revision_no == 1
    revisions = service.storage.revisions("u2")
    assert len(revisions) == 1 and revisions[0].source == "normal"


def test_backfill_waiter_converges_after_validate_crash() -> None:
    """回溯等待正常校验时对方崩溃：回溯自行补跑收敛，批次不留未执行状态。"""
    import time as _time

    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    service.register_batch("w1", [{"age": 150}, {"age": 300}])  # 只登记，未校验
    batch = service.storage.get_batch("w1")
    acquired = threading.Event()

    def crash() -> None:
        acquired.set()           # 正常校验已占执行权
        _time.sleep(0.3)         # 留出窗口让回溯进入等待状态
        raise RuntimeError("模拟正常校验中断")

    def crasher() -> None:
        try:
            run_batch(service.storage, batch, interrupt=crash)
        except RuntimeError:
            pass  # 中断方本身抛错是预期行为

    t = threading.Thread(target=crasher)
    t.start()
    acquired.wait(timeout=5)
    revision, reran = backfill_batch(service.storage, batch)  # 等待方：应自行收敛
    t.join(timeout=10)

    logger.info("正常校验崩溃后回溯自行收敛: rev=%s reran=%s source=%s",
                revision.revision_no, reran, revision.source)
    assert reran is True and revision.revision_no == 1
    revisions = service.storage.revisions("w1")
    assert len(revisions) == 1 and revisions[0].source == "backfill"
    assert service.batch_status("w1") is BatchStatus.FAILED
    assert service.get_report("w1").revision_no == 1


def test_validate_waiter_converges_after_backfill_crash() -> None:
    """正常校验等待回溯时对方崩溃：校验自行补跑收敛，不向用户抛内部错误。"""
    import time as _time

    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    service.register_batch("w2", [{"age": 150}, {"age": 300}])  # 只登记，未校验
    batch = service.storage.get_batch("w2")
    acquired = threading.Event()

    def crash() -> None:
        acquired.set()           # 回溯已占租约
        _time.sleep(0.3)         # 留出窗口让正常校验进入等待状态
        raise RuntimeError("模拟回溯中断")

    def crasher() -> None:
        try:
            backfill_batch(service.storage, batch, interrupt=crash)
        except RuntimeError:
            pass  # 中断方本身抛错是预期行为

    t = threading.Thread(target=crasher)
    t.start()
    acquired.wait(timeout=5)
    report = service.validate_batch("w2")  # 等待方：应自行收敛而非报错
    t.join(timeout=10)

    logger.info("回溯崩溃后正常校验自行收敛: rev=%s status=%s counts=%s",
                report.revision_no, report.status.value, report.counts)
    assert report.revision_no == 1 and report.status is BatchStatus.FAILED
    revisions = service.storage.revisions("w2")
    assert len(revisions) == 1 and revisions[0].source == "normal"
    assert service.storage.rerun_owner("w2") is None


def test_large_backfill_matches_one_by_one() -> None:
    """大规模批量回溯的最终结论，与逐个串行回溯完全一致，且顺序无关。"""
    # 参照：逐个串行
    ref = make_service(n_batches=25)
    for bid in [b.batch_id for b in ref.storage.list_batches()]:
        ref.backfill_one(bid)
    expected = {bid: ref.get_report(bid) for bid in
                [b.batch_id for b in ref.storage.list_batches()]}

    # 批量：打乱提交顺序 + 多并发
    svc = make_service(n_batches=25)
    ids = [b.batch_id for b in svc.storage.list_batches()][::-1]
    job = svc.backfill_batches(ids, max_workers=8)
    assert all(i.status is BackfillItemStatus.SUCCEEDED for i in job.items.values())
    for bid, exp in expected.items():
        got = svc.get_report(bid)
        assert got.status == exp.status
        assert got.counts == exp.counts
        assert got.rule_versions == exp.rule_versions == {"rg": 2}
        assert got.revision_no == 2
    # 作业内条目顺序固定为 batch_id 升序（与提交顺序无关）
    assert list(job.items) == sorted(job.items)
    logger.info("25 批次乱序+8 并发回溯，结论与逐个串行完全一致")
