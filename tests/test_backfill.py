"""历史批次回溯：新旧对照、旧结论留痕、并发去重、中断重试、失败隔离、顺序无关。"""
import logging
import threading

import pytest

from dqm.backfill import rerun_batch
from dqm.models import BatchStatus
from dqm.service import DQService

logger = logging.getLogger("test.backfill")

ROWS = [
    {"id": 1, "name": "alice", "age": 30},
    {"id": 1, "name": None, "age": 200},
    {"id": 3, "age": 40},
]


def make_service(*batch_ids: str) -> DQService:
    service = DQService()
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 200}})
    service.register_rule({"rule_id": "uq", "rule_type": "unique", "field": "id",
                           "severity": "warning"})
    for bid in batch_ids:
        service.register_batch(bid, ROWS)
        service.validate_batch(bid)
    return service


def tighten_age(service: DQService) -> None:
    # age 上限 200 -> 120：第 1 行新增一个 failure
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})


def test_backfill_compares_old_and_new_and_preserves_history() -> None:
    """回溯后可并排对照翻盘/新增/消失；旧结论不被覆盖，随时可查。"""
    service = make_service("b1")
    old_report = service.get_report("b1")
    assert old_report.counts == {"failures": 2, "warnings": 1}
    assert old_report.status.value == "failed"

    # 收紧 age 到 120：第 1 行（age=200）新增一个 range failure
    tighten_age(service)
    summary = service.backfill(["b1"])
    item = summary.items[0]
    logger.info("回溯对照: %s -> %s flipped=%s added=%s removed=%s delta=%s",
                item.old_status.value, item.new_status.value, item.flipped,
                [(c.rule_id, c.row_index, c.message) for c in item.diff.added],
                [(c.rule_id, c.row_index) for c in item.diff.removed],
                summary.delta_by_severity)

    assert item.state.value == "updated"
    assert item.old_status is BatchStatus.FAILED
    assert item.new_status is BatchStatus.FAILED
    assert item.flipped is False  # 本来就有 not_null failure
    assert len(item.diff.added) == 1 and item.diff.added[0].rule_id == "rg"
    assert item.diff.added[0].row_index == 1
    assert summary.delta_by_severity == {"failure": 1, "warning": 0}

    # 当前结论为新结论
    new_report = service.get_report("b1")
    assert new_report.execution_id == item.new_execution_id
    assert new_report.counts == {"failures": 3, "warnings": 1}

    # 再放宽回 [0,200] 并再次回溯：该 range 命中作为"消失"呈现，旧两版结论都保留
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 200}})
    loosened = service.backfill(["b1"]).items[0]
    assert loosened.state.value == "updated"
    assert len(loosened.diff.removed) == 1 and loosened.diff.removed[0].rule_id == "rg"
    assert loosened.new_status is BatchStatus.FAILED  # not_null 仍在
    assert service.get_report("b1").counts == {"failures": 2, "warnings": 1}

    # 三版结论全部保留在谱系中，可并排对照最新两版
    history = service.conclusion_history("b1")
    assert [r.execution_id for r in history] == [old_report.execution_id,
                                                 new_report.execution_id,
                                                 service.get_report("b1").execution_id]
    assert history[0].counts == {"failures": 2, "warnings": 1}
    assert history[1].counts == {"failures": 3, "warnings": 1}
    compare = service.compare_conclusions("b1")
    assert compare.batch_id == "b1" and len(compare.diff.removed) == 1


def test_backfill_flip_status() -> None:
    """规则收紧使批次从 succeeded 翻盘为 failed，新增命中与级别分布正确。"""
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 200}})
    service.register_batch("s", [{"age": 150}])
    service.validate_batch("s")
    tighten_age(service)
    item = service.backfill(["s"]).items[0]
    logger.info("翻盘: %s -> %s, added=%s, delta=%s",
                item.old_status.value, item.new_status.value,
                [(c.row_index, c.message) for c in item.diff.added],
                item.delta_by_severity)
    assert item.flipped
    assert item.old_status is BatchStatus.SUCCEEDED
    assert item.new_status is BatchStatus.FAILED
    assert len(item.diff.added) == 1


def test_backfill_is_idempotent_and_dedup_concurrent() -> None:
    """重复/并发回溯同一批次：只有一次真正生效，谱系与计数不翻倍。"""
    service = make_service("bc")
    tighten_age(service)

    first = service.backfill(["bc"]).items[0]
    assert first.state.value == "updated"
    second = service.backfill(["bc"]).items[0]
    logger.info("重复回溯: state=%s execution=%s", second.state.value,
                second.new_execution_id)
    assert second.state.value == "current"
    assert second.new_execution_id == first.new_execution_id
    assert len(service.conclusion_history("bc")) == 2  # 旧 + 新一版，无重复

    # 并发 8 线程同时回溯同一批次
    barrier = threading.Barrier(8)
    items, errors = [], []

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            items.append(service.backfill(["bc"]).items[0])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors, f"并发回溯出现错误: {errors}"
    exec_ids = {i.new_execution_id for i in items}
    states = {i.state.value for i in items}
    logger.info("并发回溯: execution 集合=%s 状态集合=%s 谱系长度=%d",
                exec_ids, states, len(service.conclusion_history("bc")))
    assert exec_ids == {first.new_execution_id}
    assert states == {"current"}
    assert len(service.conclusion_history("bc")) == 2
    assert service.get_report("bc").counts == {"failures": 3, "warnings": 1}


def test_backfill_one_failure_does_not_block_others_and_is_retryable() -> None:
    """批量回溯中一批失败：状态交代清楚、旧结论仍生效，其它批次照常更新，可单独重试。"""
    service = make_service("g1", "g2")
    tighten_age(service)

    # 让 g1 第一次回溯在执行中途中断，g2 正常
    def crash() -> None:
        raise RuntimeError("模拟回溯中断")

    summary = service.backfill(["g1", "g2"], interrupts={"g1": crash})
    by_id = {i.batch_id: i for i in summary.items}
    logger.info("部分失败: g1=%s(%s) g2=%s flipped=%s",
                by_id["g1"].state.value, by_id["g1"].error,
                by_id["g2"].state.value, by_id["g2"].flipped)

    assert by_id["g2"].state.value == "updated" and not by_id["g2"].flipped
    assert len(by_id["g2"].diff.added) == 1  # 旧结论本就 failed（not_null），不翻盘
    assert by_id["g1"].state.value == "failed"
    assert "模拟回溯中断" in by_id["g1"].error
    # g1 旧结论仍生效，无半完成结果、谱系未追加
    assert service.batch_status("g1").value == "failed"  # 旧结论本就是 failed
    assert len(service.conclusion_history("g1")) == 1
    assert service.get_report("g1").counts == {"failures": 2, "warnings": 1}

    # 单独重试 g1 到成功（旧结论本就 failed，收紧后仍 failed，不翻盘但有新增）
    retried = service.rerun_one("g1")
    logger.info("重试 g1: state=%s flipped=%s added=%d", retried.state.value,
                retried.flipped, len(retried.diff.added))
    assert retried.state.value == "updated" and not retried.flipped
    assert len(retried.diff.added) == 1
    assert len(service.conclusion_history("g1")) == 2


def test_backfill_order_independent() -> None:
    """以不同顺序回溯，每个批次的最终结论与谱系完全一致。"""
    s1 = make_service("o1", "o2", "o3")
    s2 = make_service("o1", "o2", "o3")
    tighten_age(s1)
    tighten_age(s2)

    s1.backfill(["o3", "o1", "o2"])
    s2.backfill(["o2", "o3"])
    s2.backfill(["o1"])  # 分两批、不同顺序

    for bid in ("o1", "o2", "o3"):
        h1 = [(r.status, r.counts, r.rule_versions) for r in s1.conclusion_history(bid)]
        h2 = [(r.status, r.counts, r.rule_versions) for r in s2.conclusion_history(bid)]
        logger.info("顺序无关 %s: 谱系长度=%d", bid, len(h1))
        assert h1 == h2
        assert s1.get_report(bid) == s2.get_report(bid) or (
            s1.get_report(bid).counts == s2.get_report(bid).counts)


def test_backfill_collides_with_normal_validation() -> None:
    """回溯与正常触发校验撞车：收敛到同一份结论，不翻倍、不留半成品。"""
    service = make_service("x1")
    tighten_age(service)
    barrier = threading.Barrier(2)
    results = {"backfill": None, "validate": None}
    errors = []

    def do_backfill() -> None:
        try:
            barrier.wait(timeout=5)
            results["backfill"] = service.backfill(["x1"]).items[0]
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def do_validate() -> None:
        try:
            barrier.wait(timeout=5)
            results["validate"] = service.validate_batch("x1")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=do_backfill)
    t2 = threading.Thread(target=do_validate)
    for t in (t1, t2):
        t.start()
    for t in (t1, t2):
        t.join(timeout=30)

    assert not errors, f"撞车出现错误: {errors}"
    final = service.get_report("x1")
    lineage = service.conclusion_history("x1")
    logger.info("撞车后: 谱系长度=%d 最终 counts=%s status=%s",
                len(lineage), final.counts, final.status.value)
    # 只有两种可能：回溯先生效（2 版结论），或正常校验先看到旧结论而后回溯补齐；
    # 最终当前结论必然是按新规则的 failure=3
    assert final.counts == {"failures": 3, "warnings": 1}
    assert len(lineage) == 2
    assert results["backfill"].new_execution_id == final.execution_id
    assert results["validate"].counts in (
        {"failures": 2, "warnings": 1},  # 恰好读到回溯前旧结论
        {"failures": 3, "warnings": 1},  # 复用了回溯的新结论
    )


def test_backfill_unvalidated_batch_reported_as_failed_item() -> None:
    """尚无生效结论的批次不能回溯：单项明确失败并说明原因，不连累同批其它项。"""
    service = make_service("h1")
    service.register_batch("h0", ROWS)  # 只登记未执行
    tighten_age(service)
    summary = service.backfill(["h0", "h1"])
    by_id = {i.batch_id: i for i in summary.items}
    logger.info("不可回溯项: h0=%s(%s) h1=%s",
                by_id["h0"].state.value, by_id["h0"].error, by_id["h1"].state.value)
    assert by_id["h0"].state.value == "failed"
    assert by_id["h0"].old_status is None and by_id["h0"].new_status is None
    assert "尚无生效结论" in by_id["h0"].error
    assert by_id["h1"].state.value == "updated"


def test_interrupted_rerun_releases_lock_and_recovers() -> None:
    """直接重跑中断：执行权释放、状态回到旧结论，重试后成功且只有两条谱系。"""
    service = make_service("z1")
    batch = service.storage.get_batch("z1")
    old_exec = service.storage.committed_execution("z1")
    tighten_age(service)

    with pytest.raises(RuntimeError):
        rerun_batch(service.storage, batch, interrupt=lambda: (_ for _ in ()).throw(
            RuntimeError("crash")))

    # 中断后旧结论仍在、批次状态明确、无半完成结果
    assert service.storage.committed_execution("z1").execution_id == old_exec.execution_id
    assert len(service.conclusion_history("z1")) == 1
    assert service.batch_status("z1").value == "failed"

    new_exec, prior, ran = rerun_batch(service.storage, batch)
    logger.info("中断后重试: ran=%s prior=%s new=%s",
                ran, prior.execution_id, new_exec.execution_id)
    assert ran and prior.execution_id == old_exec.execution_id
    assert len(service.conclusion_history("z1")) == 2
