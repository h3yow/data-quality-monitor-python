"""规则升级影响试算：只读、可重复、翻盘与级别分布、非法/冲突当场拒绝。"""
import logging

import pytest

from dqm.errors import TrialRejected, WorkloadTooLarge
from dqm.models import RejectReason
from dqm.service import DQService
from dqm.storage import ReadOnlyStorageView
from dqm import trial as trial_mod

logger = logging.getLogger("test.trial")


def make_service() -> DQService:
    """旧规则 age∈[0,200]：b_old 通过，b_bad 失败；待收紧到 [0,120]。"""
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 200}})
    service.register_batch("b_old", [{"age": 150}])          # 旧通过 -> 新失败（翻盘）
    service.register_batch("b_bad", [{"age": 300}])          # 旧失败 -> 新仍失败（不翻盘）
    service.register_batch("b_ok", [{"age": 30}])            # 始终通过
    service.validate_batch("b_old")
    service.validate_batch("b_bad")
    service.validate_batch("b_ok")
    return service


TIGHTEN = {"rule_id": "rg", "rule_type": "range", "field": "age",
           "params": {"min": 0, "max": 120}}


def log_result(tag: str, result) -> None:
    logger.info("[%s] 候选快照=%s 翻盘=%s 受影响=%s 新增分布=%s 消失分布=%s",
                tag, result.rule_versions, result.flipped_batch_ids,
                result.affected_batch_ids, result.added_by_severity,
                result.removed_by_severity)
    for imp in result.impacts:
        logger.info("[%s] 批次=%s 旧%s->新%s 翻盘=%s 新增=%d 消失=%d",
                    tag, imp.batch_id, imp.old_status.value, imp.new_status.value,
                    imp.flipped, len(imp.added), len(imp.removed))


def test_trial_detects_flips_counts_and_severity_distribution() -> None:
    """试算准确指出翻盘批次、每批新增/消失命中及严重级别分布。"""
    service = make_service()
    result = service.trial_rule(TIGHTEN)
    log_result("收紧区间", result)

    assert result.flipped_batch_ids == ("b_old",)           # 仅 150 由通过变失败
    assert set(result.affected_batch_ids) == {"b_old", "b_bad"}
    by_id = {i.batch_id: i for i in result.impacts}
    assert by_id["b_old"].old_status.value == "succeeded"
    assert by_id["b_old"].new_status.value == "failed"
    assert len(by_id["b_old"].added) == 1 and len(by_id["b_old"].removed) == 0
    # b_bad：同一条命中的版本/依据变了 -> 旧命中消失、新命中新增，但状态不翻盘
    assert by_id["b_bad"].flipped is False
    assert len(by_id["b_bad"].added) == 1 and len(by_id["b_bad"].removed) == 1
    # b_ok：完全不受波及
    assert "b_ok" not in result.affected_batch_ids
    assert result.added_by_severity == {"failure": 2, "warning": 0}
    assert result.removed_by_severity == {"failure": 1, "warning": 0}


def test_trial_is_read_only() -> None:
    """试算绝不改动生效规则版本，也不改写任何历史批次结论。"""
    service = make_service()
    before = {bid: service.get_report(bid) for bid in ("b_old", "b_bad", "b_ok")}

    service.trial_rule(TIGHTEN)
    service.trial_rule(TIGHTEN)  # 随便重复跑

    assert service.storage.current_version("rg") == 1      # 规则未升版
    for bid, rep_before in before.items():
        rep_after = service.get_report(bid)
        assert rep_after == rep_before, f"批次 {bid} 结论被试算篡改"
        assert rep_after.rule_versions == {"rg": 1}        # 仍是 v1 结论
        assert len(service.storage.revisions(bid)) == 1     # 未新增结论版本
    logger.info("试算后规则仍为 v1，三个批次结论与版本数均未变化（只读成立）")


def test_trial_repeatable_and_order_deterministic() -> None:
    """同一待定规则重复试算结果按值完全一致，且批次顺序固定。"""
    service = make_service()
    r1 = service.trial_rule(TIGHTEN)
    r2 = service.trial_rule(TIGHTEN)
    assert r1 == r2
    assert [i.batch_id for i in r1.impacts] == ["b_bad", "b_ok", "b_old"]
    logger.info("两次试算结果按值相等；批次顺序=%s",
                [i.batch_id for i in r1.impacts])


def test_trial_includes_unexecuted_batch_with_empty_baseline() -> None:
    """从未校验的登记批次也要被纳入：旧基线为空，命中全部计入新增。"""
    service = DQService()  # 尚无任何规则
    service.register_batch("fresh", [{"name": "a"}, {}])  # 登记但不触发 validate
    result = service.trial_rule(
        {"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    imp = result.impacts[0]
    assert imp.batch_id == "fresh" and imp.had_conclusion is False
    assert imp.old_status.value == "registered"
    assert len(imp.added) == 1 and len(imp.removed) == 0
    assert imp.flipped is False  # 无旧结论不算"翻盘"
    logger.info("未执行批次试算: 旧基线空 -> 新增命中=%d", len(imp.added))


def test_trial_rejects_invalid_candidate() -> None:
    """待定规则定义非法：当场拒绝（invalid_definition），不给半截结果。"""
    service = make_service()
    with pytest.raises(TrialRejected) as exc:
        service.trial_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                            "params": {"min": 100, "max": 0}})
    assert exc.value.reason is RejectReason.INVALID_DEFINITION
    logger.info("非法候选被拒绝: %s", exc.value.detail)
    assert service.trial_rule(TIGHTEN).flipped_batch_ids == ("b_old",)  # 系统未受污染


def test_trial_rejects_conflicting_candidate() -> None:
    """待定新规则与现行规则打架：当场拒绝（rule_conflict）。"""
    service = DQService()
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    service.register_batch("b", [{"name": "a"}])
    with pytest.raises(TrialRejected) as exc:
        service.trial_rule({"rule_id": "nn2", "rule_type": "not_null", "field": "name"})
    assert exc.value.reason is RejectReason.RULE_CONFLICT
    logger.info("冲突候选被拒绝: %s", exc.value.detail)
    assert service.storage.current_version("nn2") == 0


def test_trial_candidate_upgrade_same_id_is_not_self_conflict() -> None:
    """同 rule_id 的升级候选与自身旧版本不构成冲突，可以正常试算。"""
    service = make_service()
    result = service.trial_rule(TIGHTEN)  # rg 升级，id 相同
    assert result.rule_versions == {"rg": 2}
    assert result.flipped_batch_ids == ("b_old",)


def test_trial_new_rule_addition_trial() -> None:
    """新增一条独立规则（新 id）：与现行规则叠加试算。"""
    service = make_service()
    result = service.trial_rule(
        {"rule_id": "nn", "rule_type": "not_null", "field": "name",
         "severity": "warning"})
    assert result.rule_versions == {"rg": 1, "nn": 1}
    log_result("新增规则", result)
    # 三批数据都没有 name 字段 -> 每批新增 warning
    assert result.added_by_severity["warning"] == 3


def test_trial_only_holds_read_only_view() -> None:
    """结构性保证：试算内部拿到的是只读视图，不存在任何写方法。"""
    service = make_service()
    captured: dict[str, object] = {}
    original = trial_mod.ReadOnlyStorageView
    try:
        class Spy(original):
            def __init__(self, storage):
                super().__init__(storage)
                captured["view"] = self
        trial_mod.ReadOnlyStorageView = Spy  # type: ignore[misc]
        service.trial_rule(TIGHTEN)
    finally:
        trial_mod.ReadOnlyStorageView = original  # type: ignore[misc]
    view = captured["view"]
    for write_attr in ("append_rule_version", "add_batch", "commit_execution",
                       "abort_execution", "commit_rerun", "abort_rerun",
                       "acquire_rerun_if_stale", "save_backfill_job"):
        assert not hasattr(view, write_attr), f"只读视图泄露写方法 {write_attr}"
    assert isinstance(view, ReadOnlyStorageView)
    logger.info("试算仅持有 ReadOnlyStorageView，写方法全部不可达")


def test_trial_rejects_when_batch_count_exceeds_bound(monkeypatch) -> None:
    """批次数量超过处理上界：明确拒绝，不截断、不给半截结果。"""
    service = make_service()
    monkeypatch.setattr(trial_mod, "MAX_TRIAL_BATCHES", 2)
    with pytest.raises(WorkloadTooLarge):
        service.trial_rule(TIGHTEN)
    logger.info("批次数超上界被明确拒绝（WorkloadTooLarge）")


def test_trial_rejects_oversized_batch(monkeypatch) -> None:
    """单个批次行数超上界：试算当场拒绝，且不改动任何数据。"""
    service = DQService()
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    service.register_batch("big", [{"name": "a"}, {"name": "b"}, {}])
    monkeypatch.setattr(trial_mod, "MAX_ROWS_PER_BATCH", 2)
    with pytest.raises(WorkloadTooLarge):
        service.trial_rule(
            {"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    # 拒绝后无结论版本被写出
    assert service.storage.latest_revision("big") is None
    logger.info("超大单批试算被拒绝，无任何写出")
