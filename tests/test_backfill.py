"""历史批次回溯：结论版本化对照、并发唯一生效、中断重试、失败隔离、有界一致性。"""
import logging
import threading

import pytest

from dqm import backfill as bm
from dqm.backfill import backfill_batch, backfill_batches
from dqm.errors import BackfillError
from dqm.executor import evaluate
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


def test_backfill_job_ledger_distinguishes_rerun_and_dedup() -> None:
    """作业账本如实区分：真重跑产出新结论（reran=True）vs 快照已最新被去重（reran=False）。"""
    service = make_service(n_batches=3)  # 规则已收紧到 v2，历史批次仍是 v1 结论

    job1 = service.backfill_batches(["b000", "b001", "b002"], max_workers=3)
    for bid, item in job1.items.items():
        assert item.status is BackfillItemStatus.SUCCEEDED
        assert item.reran is True, f"{bid} 应标记为真重跑"
        assert item.revision_no == 2
    logger.info("首轮回溯账本（规则已收紧）: %s",
                {k: {"reran": v.reran, "rev": v.revision_no}
                 for k, v in job1.items.items()})

    # 规则未再变化：同样的批次再回溯一次，应全部被去重，不再产生新版本
    job2 = service.backfill_batches(["b000", "b001", "b002"], max_workers=3)
    for bid, item in job2.items.items():
        assert item.status is BackfillItemStatus.SUCCEEDED
        assert item.reran is False, f"{bid} 应标记为去重跳过"
        assert item.revision_no == 2  # 仍指向既有 v2，未追加新版本
    assert all(len(service.storage.revisions(bid)) == 2
               for bid in ("b000", "b001", "b002"))
    logger.info("次轮回溯账本（规则未变）: %s —— 与首轮清晰可区分",
                {k: {"reran": v.reran, "rev": v.revision_no}
                 for k, v in job2.items.items()})

    # 混合场景：只收紧后再回溯，含已最新批次的作业里两种标记并存
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 100}})
    service.backfill_one("b000")  # b000 先单独回溯到 v3
    job3 = service.backfill_batches(["b000", "b001"], max_workers=2)
    assert job3.items["b000"].reran is False   # 已是 v3，去重
    assert job3.items["b000"].revision_no == 3
    assert job3.items["b001"].reran is True    # 从 v2 真重跑到 v3
    assert job3.items["b001"].revision_no == 3
    logger.info("混合账本: b000 reran=%s rev=%s, b001 reran=%s rev=%s",
                job3.items["b000"].reran, job3.items["b000"].revision_no,
                job3.items["b001"].reran, job3.items["b001"].revision_no)


def test_backfill_and_validate_race_on_unvalidated_batch() -> None:
    """未校验批次上回溯持租约、正常校验同时进来：只留一条生效结论，两边口径一致。"""
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    service.register_batch("u1", [{"age": 150}, {"age": 300}])  # 只登记，不校验
    batch = service.storage.get_batch("u1")
    entered = threading.Event()
    proceed = threading.Event()
    results = {}

    def backfiller() -> None:
        original_evaluate = bm.evaluate

        def slow_evaluate(rows, snapshot):
            entered.set()              # 租约已持有，正在求值
            proceed.wait(timeout=5)
            return original_evaluate(rows, snapshot)

        bm.evaluate = slow_evaluate  # type: ignore[misc]
        try:
            results["backfill"] = backfill_batch(service.storage, batch)
        finally:
            bm.evaluate = original_evaluate  # type: ignore[misc]

    def validator() -> None:
        entered.wait(timeout=5)
        # 回溯已占租约；正常校验不得另起执行，应等待并复用回溯结论
        results["validate"] = service.validate_batch("u1")

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

    revision, reran = results["backfill"]
    assert reran is True and revision.revision_no == 1  # 首版结论由回溯产出
    report = results["validate"]
    assert report.revision_no == 1
    assert report.execution_id == revision.execution_id  # 同一份生效结论
    assert report.rule_versions == {"rg": 1}
    # 关键：整个批次只有一条生效结论，不存在"正常一条 + 回溯一条"
    revisions = service.storage.revisions("u1")
    assert len(revisions) == 1 and revisions[0].source == "backfill"
    logger.info("未校验批次撞车收敛：唯一结论 rev1(source=backfill) "
                "execution=%s，两边拿到同一份", revision.execution_id[:8])


def test_backfill_validate_race_either_order_single_conclusion() -> None:
    """未校验批次上回溯与正常校验并发（先后不定）：无论谁抢到，最终只有一条结论。"""
    import time as _time

    import dqm.executor as ex

    # 把两边的求值都放慢，放大"一方持租约、另一方进来"的撞车窗口
    original_bf_evaluate = bm.evaluate
    original_ex_evaluate = ex.evaluate

    def slow_evaluate(rows, snapshot, _orig=None):
        _time.sleep(0.01)
        return (_orig or original_bf_evaluate)(rows, snapshot)

    for round_no in range(20):
        service = DQService()
        service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                               "params": {"min": 0, "max": 120}})
        bid = f"r{round_no}"
        service.register_batch(bid, [{"age": 150}, {"age": 300}])
        batch = service.storage.get_batch(bid)
        barrier = threading.Barrier(2)
        results, errors = {}, []

        def backfiller() -> None:
            try:
                barrier.wait(timeout=5)
                results["backfill"] = backfill_batch(service.storage, batch)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def validator() -> None:
            try:
                barrier.wait(timeout=5)
                results["validate"] = service.validate_batch(bid)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        bm.evaluate = slow_evaluate  # type: ignore[misc]
        ex.evaluate = lambda rows, snapshot: slow_evaluate(  # type: ignore[misc]
            rows, snapshot, _orig=original_ex_evaluate)
        try:
            threads = [threading.Thread(target=backfiller),
                       threading.Thread(target=validator)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
        finally:
            bm.evaluate = original_bf_evaluate  # type: ignore[misc]
            ex.evaluate = original_ex_evaluate  # type: ignore[misc]

        assert not errors, f"第 {round_no} 轮出现错误: {errors}"
        revisions = service.storage.revisions(bid)
        assert len(revisions) == 1, (
            f"第 {round_no} 轮出现 {len(revisions)} 条生效结论: "
            f"{[(r.revision_no, r.source) for r in revisions]}")
        revision, _ = results["backfill"]
        report = results["validate"]
        assert revision.execution_id == report.execution_id == revisions[0].execution_id
        assert report.revision_no == 1
    logger.info("20 轮乱序撞车（求值放慢）：每轮都只有一条生效结论，两边拿到同一份")


def test_concurrent_backfill_waiting_side_recovers_after_interrupt() -> None:
    """并发回溯中抢到租约的一次被中断：等待方自己补跑，收敛出唯一新结论。"""
    service = make_service(n_batches=1)  # b000 仍是 v1 旧结论，规则已收紧
    batch = service.storage.get_batch("b000")
    barrier = threading.Barrier(2)
    calls = {"n": 0}
    results, errors = [], []

    def interrupt_once() -> None:
        # 只有第一个进入执行体（即抢到租约）的回溯被中断；
        # 先挂起片刻，确保另一个线程已进入等待路径
        calls["n"] += 1
        if calls["n"] == 1:
            import time as _time
            _time.sleep(0.15)
            raise RuntimeError("模拟回溯中断")

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(backfill_batch(service.storage, batch,
                                          interrupt=interrupt_once))
        except RuntimeError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    # 被中断的执行方抛出；等待方不抛内部错误，自己补跑成功
    assert len(errors) == 1 and "模拟回溯中断" in str(errors[0])
    assert len(results) == 1
    revision, reran = results[0]
    assert reran is True and revision.revision_no == 2
    # 批次收敛到唯一新结论：v1(旧) + v2(补跑)，无半完成版本、租约已释放
    revisions = service.storage.revisions("b000")
    assert [r.revision_no for r in revisions] == [1, 2]
    assert service.storage.rerun_owner("b000") is None
    assert service.batch_status("b000") is BatchStatus.FAILED
    assert service.get_report("b000").revision_no == 2
    logger.info("并发回溯一方中断：等待方补跑产出 v2，版本轨迹=%s",
                [(r.revision_no, r.source) for r in revisions])


def test_backfill_waiter_recovers_on_unvalidated_batch_after_interrupt() -> None:
    """未校验批次上并发回溯、抢到租约的一次被中断：等待方自己补跑，批次不留空窗。"""
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    service.register_batch("u9", [{"age": 150}, {"age": 300}])  # 只登记，无任何结论
    batch = service.storage.get_batch("u9")
    barrier = threading.Barrier(2)
    calls = {"n": 0}
    results, errors = [], []

    def interrupt_once() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            import time as _time
            _time.sleep(0.15)  # 确保另一个线程已进入等待路径
            raise RuntimeError("模拟回溯中断")

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(backfill_batch(service.storage, batch,
                                          interrupt=interrupt_once))
        except RuntimeError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    # 等待方不得收到 TimeoutError/内部错误，应自己补跑出首版结论
    assert len(errors) == 1 and "模拟回溯中断" in str(errors[0])
    assert len(results) == 1
    revision, reran = results[0]
    assert reran is True and revision.revision_no == 1
    revisions = service.storage.revisions("u9")
    assert len(revisions) == 1 and revisions[0].source == "backfill"
    assert service.storage.rerun_owner("u9") is None
    assert service.batch_status("u9") is BatchStatus.FAILED  # 不停在 registered
    logger.info("未校验批次上等待方自愈：补跑产出首版结论 rev1，批次未卡住")


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
