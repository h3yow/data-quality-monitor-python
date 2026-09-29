"""规则升级影响试算：只读可重复、翻盘/增删/级别分布准确、非法与冲突当场拒绝。"""
import logging

import pytest

from dqm.errors import RuleRejected, ScopeExceeded
from dqm.models import RejectReason, RuleSpec, RuleType
from dqm.service import DQService

logger = logging.getLogger("test.trial")

ROWS_FLIP = [{"name": "alice", "age": 150}]   # [0,200] 通过 -> [0,120] 失败
ROWS_KEEP = [{"name": "bob", "age": 30}]      # 两版规则下都通过


def make_service() -> DQService:
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 200}})
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    service.register_batch("flip", ROWS_FLIP)
    service.register_batch("keep", ROWS_KEEP)
    service.register_batch("pending", ROWS_FLIP)  # 只登记不执行
    service.validate_batch("flip")
    service.validate_batch("keep")
    return service


def tighten() -> dict:
    return {"rule_id": "rg", "rule_type": "range", "field": "age",
            "params": {"min": 0, "max": 120}}


def test_trial_is_read_only_and_repeatable() -> None:
    """试算任意次结果一致，且绝不修改生效规则版本与历史批次结论。"""
    service = make_service()
    before_rule_version = service.storage.current_version("rg")
    before_report = service.get_report("flip")

    r1 = service.trial_rule(tighten())
    r2 = service.trial_rule(tighten())
    r3 = service.trial_rule(tighten(), ["flip", "keep"])
    logger.info("试算结果1: 翻盘=%s 受影响=%s 跳过=%s",
                r1.flipped_batches, r1.affected_batches, r1.skipped)

    assert r1 == r2, "只读试算重复运行必须逐字一致"
    # 只读：规则未发布、历史结论未被改动
    assert service.storage.current_version("rg") == before_rule_version == 1
    assert service.get_report("flip") == before_report
    assert service.get_report("flip").status.value == "succeeded"
    assert service.batch_status("flip").value == "succeeded"
    # 候选版本号是"假设发布后"的 v2，但并未落盘
    assert r1.candidate_version == 2 and r1.rule_versions["rg"] == 2

    # 显式指定子集时结果只含该子集，且与全量试算中对应批次一致
    assert {i.batch_id for i in r3.evaluated} == {"flip", "keep"}
    full_by_id = {i.batch_id: i for i in r1.evaluated}
    sub_by_id = {i.batch_id: i for i in r3.evaluated}
    assert sub_by_id == {"flip": full_by_id["flip"], "keep": full_by_id["keep"]}


def test_trial_reports_flips_added_removed_and_severity_distribution() -> None:
    """翻盘批次、新增/消失命中、按严重级别的净变化分布必须准确。"""
    service = make_service()
    report = service.trial_rule(tighten())
    by_id = {i.batch_id: i for i in report.evaluated}

    flip = by_id["flip"]
    keep = by_id["keep"]
    logger.info("flip 批次: %s -> %s flipped=%s added=%s removed=%s delta=%s",
                flip.old_status.value, flip.new_status.value, flip.flipped,
                [(c.rule_id, c.row_index, c.message) for c in flip.diff.added],
                [(c.rule_id, c.row_index) for c in flip.diff.removed],
                flip.delta_by_severity)

    assert report.flipped_batches == ("flip",)
    assert report.affected_batches == ("flip",)
    assert flip.old_status.value == "succeeded" and flip.new_status.value == "failed"
    assert len(flip.diff.added) == 1 and flip.diff.added[0].row_index == 0
    assert flip.diff.added[0].severity.value == "failure"
    assert flip.diff.removed == () and flip.diff.modified == ()
    assert flip.delta_by_severity == {"failure": 1, "warning": 0}
    # 不受波及的批次：无翻盘、无增删
    assert not keep.flipped and not keep.diff.changed()
    assert keep.delta_by_severity == {"failure": 0, "warning": 0}
    # 尚无生效结论的批次被明确标记跳过，而不是悄悄漏掉
    assert report.skipped == ("pending",)


def test_trial_new_rule_adds_findings() -> None:
    """候选是一条全新规则（现行不存在）时，版本假设为 v1，命中作为新增呈现。"""
    service = make_service()
    payload = {"rule_id": "uqe", "rule_type": "unique", "field": "name",
               "severity": "warning"}
    service.register_batch("dup", [{"name": "x"}, {"name": "x"}, {"name": "y"}])
    service.validate_batch("dup")
    report = service.trial_rule(payload)
    impact = next(i for i in report.evaluated if i.batch_id == "dup")
    logger.info("新规则试算: delta=%s added=%s",
                impact.delta_by_severity,
                [(c.rule_id, c.row_index) for c in impact.diff.added])
    assert report.candidate_version == 1
    assert len(impact.diff.added) == 1 and impact.diff.added[0].severity.value == "warning"
    assert impact.delta_by_severity["warning"] == 1
    assert not impact.flipped  # 只有告警，不翻盘


def test_trial_invalid_candidate_rejected_before_any_evaluation() -> None:
    """候选规则本身非法：当场明确拒绝，不返回半截结果、不留任何改动。"""
    service = make_service()
    # 正则无法编译
    bad_regex = {"rule_id": "rx", "rule_type": "regex", "field": "name",
                 "params": {"pattern": "("}}
    with pytest.raises(RuleRejected) as exc:
        service.trial_rule(bad_regex)
    assert exc.value.reason is RejectReason.INVALID_DEFINITION
    logger.info("非法候选被拒绝: reason=%s detail=%s",
                exc.value.reason.value, exc.value.detail)
    # min > max
    with pytest.raises(RuleRejected):
        service.trial_rule(tighten() | {"params": {"min": 9, "max": 1}})
    # 缺必填参数
    with pytest.raises(RuleRejected) as exc2:
        service.trial_rule({"rule_id": "rx", "rule_type": "regex", "field": "name"})
    assert exc2.value.reason is RejectReason.MISSING_FIELD
    # 拒绝后现行规则与历史结论原封不动
    assert service.storage.current_version("rg") == 1
    assert service.get_report("flip").status.value == "succeeded"


def test_trial_conflicting_candidate_rejected() -> None:
    """候选与现行规则打架：以 RULE_CONFLICT 当场拒绝（假设发布后的规则集）。"""
    service = make_service()
    # 同字段已有 RANGE，再提一个互不相交的 ENUM
    payload = {"rule_id": "color", "rule_type": "enum", "field": "age",
               "params": {"allowed": [999]}}
    with pytest.raises(RuleRejected) as exc:
        service.trial_rule(payload)
    assert exc.value.reason is RejectReason.RULE_CONFLICT
    logger.info("冲突候选被拒绝: %s", exc.value.detail)
    # 同字段重复类型也判冲突
    with pytest.raises(RuleRejected) as exc2:
        service.trial_rule({"rule_id": "rg2", "rule_type": "range", "field": "age",
                            "params": {"min": 0, "max": 5}})
    assert exc2.value.reason is RejectReason.RULE_CONFLICT
    # 变更同 rule_id 自身不算自冲突（它是被替换对象）
    assert service.trial_rule(tighten()).candidate_rule_id == "rg"


def test_trial_scope_exceeded_rejects_whole_run() -> None:
    """批次基数超上界：整体拒绝并说明上界，不截断、不给半截结果。"""
    import dqm.trial as trial_mod

    service = make_service()
    original = trial_mod.MAX_TRIAL_BATCHES
    trial_mod.MAX_TRIAL_BATCHES = 1
    try:
        with pytest.raises(ScopeExceeded):
            service.trial_rule(tighten())
    finally:
        trial_mod.MAX_TRIAL_BATCHES = original
