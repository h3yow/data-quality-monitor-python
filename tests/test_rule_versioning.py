"""规则版本化：变更后历史批次按当时快照复现，新旧结论可对照、可解释。"""

from __future__ import annotations

import logging

from dq_monitor.models import ExecutionStatus
from dq_monitor.service import DataQualityService

logger = logging.getLogger("dq.tests.versioning")


def _setup(service: DataQualityService) -> None:
    service.register_schema("orders", [{"name": "amount", "dtype": "int"}])
    service.register_rule("amt", "orders", "amount", "RANGE",
                          "ERROR", {"min": 0, "max": 100})


def test_history_reproduces_with_old_version_after_rule_change(
    service: DataQualityService,
) -> None:
    _setup(service)
    rows = [{"amount": 150}, {"amount": 50}, {"amount": 250}]
    logger.info("输入批次 rows=%s；当时规则 amt v1: RANGE [0,100]", rows)
    service.register_batch("b1", "orders", rows)
    ex, _ = service.submit("b1")
    assert [s["version"] for s in ex.rule_snapshot] == [1]
    historical_findings = service.list_findings("b1")
    historical_conclusion = service.conclusion("b1")
    assert historical_conclusion["error_count"] == 2  # 150, 250

    # 规则变更：放宽上界到 200 -> v2
    rv2 = service.register_rule("amt", "orders", "amount", "RANGE",
                                "ERROR", {"min": 0, "max": 200})
    assert rv2.version == 2
    assert service.store.get_rule_versions("amt")[0].status.value == "SUPERSEDED"

    # 历史结论不变：仍按 v1 快照复现
    replay = service.replay("b1")
    logger.info("规则变更后回放: basis=%s 快照版本=%s 结论=%s errors=%d",
                replay["basis"],
                [(s["rule_id"], s["version"]) for s in replay["snapshot_rules"]],
                replay["conclusion"], replay["error_count"])
    assert replay["matches_historical"] is True
    assert replay["error_count"] == 2
    assert replay["findings"] == historical_findings
    assert service.conclusion("b1")["error_count"] == 2

    # 同一份数据若按新规则 v2 评判：只有 250 违规（可对照解释）
    service.register_batch("b2", "orders", rows)
    ex2, _ = service.submit("b2")
    assert [s["version"] for s in ex2.rule_snapshot] == [2]
    logger.info("新批次 b2 按 v2 评判 error_count=%d", service.conclusion("b2")["error_count"])
    assert service.conclusion("b2")["error_count"] == 1

    # 两个批次的发现都稳定可查
    assert len(service.list_findings("b1")) == 2
    assert len(service.list_findings("b2")) == 1
    assert service.stats().rule_versions == 2


def test_replay_single_rule_version_and_unknown_version(
    service: DataQualityService,
) -> None:
    _setup(service)
    service.register_batch("b1", "orders", [{"amount": 150}])
    service.submit("b1")
    service.register_rule("amt", "orders", "amount", "RANGE",
                          "ERROR", {"min": 0, "max": 200})

    # 快照里只有 v1；指定 v2 回放在历史批次上必须被拒绝并解释原因
    from dq_monitor.models import DQError, ErrorCode

    try:
        service.replay("b1", rule_version=2)
        raise AssertionError("应拒绝：v2 不属于该历史执行的快照")
    except DQError as e:
        assert e.code is ErrorCode.RULE_NOT_FOUND
        logger.info("判定依据: %s", e.message)

    replay_v1 = service.replay("b1", rule_version=1)
    assert replay_v1["error_count"] == 1
    assert replay_v1["snapshot_rules"][0]["version"] == 1


def test_multiple_rule_changes_keep_full_history(service: DataQualityService) -> None:
    _setup(service)
    service.register_batch("b1", "orders", [{"amount": 150}])
    service.submit("b1")                       # v1 [0,100] -> FAIL
    service.register_rule("amt", "orders", "amount", "RANGE",
                          "ERROR", {"min": 0, "max": 200})   # v2
    service.register_batch("b2", "orders", [{"amount": 150}])
    service.submit("b2")                       # v2 [0,200] -> PASS
    service.register_rule("amt", "orders", "amount", "RANGE",
                          "ERROR", {"min": 0, "max": 300})   # v3
    service.register_batch("b3", "orders", [{"amount": 250}])
    service.submit("b3")                       # v3 -> PASS

    versions = [v.version for v in service.store.get_rule_versions("amt")]
    assert versions == [1, 2, 3]
    logger.info("历史版本链=%s，各批次结论=%s",
                versions,
                {b: service.conclusion(b)["conclusion"] for b in ("b1", "b2", "b3")})
    assert service.conclusion("b1")["conclusion"] == "FAIL"
    assert service.conclusion("b2")["conclusion"] == "PASS"
    assert service.conclusion("b3")["conclusion"] == "PASS"
    # b1 始终绑定 v1
    ex1 = service.get_execution("b1")
    assert ex1.rule_snapshot[0]["version"] == 1
    assert service.replay("b1")["matches_historical"] is True
