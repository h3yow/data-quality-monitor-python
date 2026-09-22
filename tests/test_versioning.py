"""规则版本化：规则变更后历史批次结论仍按当时生效规则复现，查询稳定一致。"""
import logging

from dqm.executor import evaluate
from dqm.service import DQService

logger = logging.getLogger("test.versioning")

ROWS = [{"name": "alice", "age": 150}]  # age=150：旧规则 [0,200] 通过，新规则 [0,120] 失败


def make_service() -> DQService:
    service = DQService()
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 200}})
    return service


def test_historical_batch_keeps_conclusion_after_rule_change() -> None:
    """批次按执行时的规则快照定论；之后规则变更不影响历史结论。"""
    service = make_service()
    service.register_batch("old", ROWS)
    before = service.validate_batch("old")
    logger.info("规则变更前: 输入=%s, 判定依据 rule_versions=%s -> status=%s counts=%s",
                ROWS, before.rule_versions, before.status.value, before.counts)
    assert before.rule_versions == {"rg": 1}
    assert before.status.value == "succeeded"

    # 规则变更：收紧区间 -> rg 升到 v2
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    assert service.storage.current_version("rg") == 2

    # 历史批次结论不变且可复现：用执行时快照重新求值，结果一致
    after = service.get_report("old")
    logger.info("规则变更后查历史: rule_versions=%s -> status=%s（结论不变）",
                after.rule_versions, after.status.value)
    assert after.rule_versions == {"rg": 1}
    assert after.status.value == "succeeded"
    assert after.counts == before.counts

    snapshot = [service.storage.get_rule_version("rg", v)
                for v in after.rule_versions.values()]
    replayed = evaluate(ROWS, snapshot)
    logger.info("按 v1 快照复现: findings=%s（与留痕一致）", replayed)
    assert len(replayed) == len(after.failures) + len(after.warnings) == 0

    # 新批次适用新规则
    service.register_batch("new", ROWS)
    new_report = service.validate_batch("new")
    logger.info("新批次: 输入=%s, 判定依据 rule_versions=%s -> status=%s, 发现=%s",
                ROWS, new_report.rule_versions, new_report.status.value,
                [f.message for f in new_report.failures])
    assert new_report.rule_versions == {"rg": 2}
    assert new_report.status.value == "failed"
    assert new_report.counts == {"failures": 1, "warnings": 0}


def test_severity_classification_stable_across_queries() -> None:
    """失败与告警按严重级别分类；重复查询结果稳定一致。"""
    service = DQService()
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    service.register_rule({"rule_id": "uq", "rule_type": "unique", "field": "id",
                           "severity": "warning"})
    rows = [{"id": 1, "name": "a"}, {"id": 1}, {"id": 2, "name": None}]
    logger.info("输入数据: %s", rows)
    service.register_batch("b", rows)
    first = service.validate_batch("b")
    logger.info("判定依据: failures=%s, warnings=%s",
                [(f.rule_id, f.row_index, f.message) for f in first.failures],
                [(f.rule_id, f.row_index, f.message) for f in first.warnings])
    assert len(first.failures) == 2 and first.failures[0].severity.value == "failure"
    assert len(first.warnings) == 1 and first.warnings[0].severity.value == "warning"

    for _ in range(3):
        again = service.get_report("b")
        assert again == first, "重复查询结论必须稳定一致"
