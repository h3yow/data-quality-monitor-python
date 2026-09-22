"""规则登记：非法定义、字段缺失、规则冲突须按原因区分并拒绝。"""
import logging

import pytest

from dqm.errors import RuleRejected
from dqm.models import RejectReason
from dqm.service import DQService

logger = logging.getLogger("test.rules")


@pytest.fixture
def service() -> DQService:
    return DQService()


def _expect_reject(service: DQService, payload: dict, reason: RejectReason) -> None:
    logger.info("输入规则: %s, 期望拒绝原因: %s", payload, reason.value)
    with pytest.raises(RuleRejected) as exc_info:
        service.register_rule(payload)
    logger.info("实际拒绝: reason=%s, detail=%s",
                exc_info.value.reason.value, exc_info.value.detail)
    assert exc_info.value.reason is reason


def test_missing_field_rejected(service: DQService) -> None:
    _expect_reject(service, {"rule_type": "not_null", "field": "a"},
                   RejectReason.MISSING_FIELD)  # 缺 rule_id
    _expect_reject(service, {"rule_id": "r", "rule_type": "regex", "field": "e"},
                   RejectReason.MISSING_FIELD)  # 缺 params.pattern
    _expect_reject(service, {"rule_id": "r", "rule_type": "range",
                             "field": "n", "params": {"min": 0}},
                   RejectReason.MISSING_FIELD)  # 缺 params.max


def test_invalid_definition_rejected(service: DQService) -> None:
    _expect_reject(service, {"rule_id": "r", "rule_type": "range", "field": "n",
                             "params": {"min": 10, "max": 1}},
                   RejectReason.INVALID_DEFINITION)  # min > max
    _expect_reject(service, {"rule_id": "r", "rule_type": "regex", "field": "e",
                             "params": {"pattern": "([a-z"}},
                   RejectReason.INVALID_DEFINITION)  # 正则无法编译
    _expect_reject(service, {"rule_id": "r", "rule_type": "enum", "field": "s",
                             "params": {"allowed": []}},
                   RejectReason.INVALID_DEFINITION)  # 空允许集合
    _expect_reject(service, {"rule_id": "r", "rule_type": "nope", "field": "s"},
                   RejectReason.INVALID_DEFINITION)  # 未知规则类型


def test_rule_conflict_rejected(service: DQService) -> None:
    service.register_rule({"rule_id": "nn", "rule_type": "not_null", "field": "name"})
    service.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                           "params": {"min": 0, "max": 120}})
    # 同字段同类型重复定义
    _expect_reject(service, {"rule_id": "nn2", "rule_type": "not_null", "field": "name"},
                   RejectReason.RULE_CONFLICT)
    # 区间互不相交
    _expect_reject(service, {"rule_id": "rg2", "rule_type": "range", "field": "age",
                             "params": {"min": 200, "max": 300}},
                   RejectReason.RULE_CONFLICT)
    # ENUM 与 RANGE 取值域互不相交
    service.register_rule({"rule_id": "rg3", "rule_type": "range", "field": "score",
                           "params": {"min": 0, "max": 100}})
    _expect_reject(service, {"rule_id": "en", "rule_type": "enum", "field": "score",
                             "params": {"allowed": [200, 300]}},
                   RejectReason.RULE_CONFLICT)


def test_rejections_do_not_leak_partial_rules(service: DQService) -> None:
    """被拒绝的规则不得进入生效规则集（不静默跳过、不留残渣）。"""
    _expect_reject(service, {"rule_id": "bad", "rule_type": "regex", "field": "e",
                             "params": {"pattern": "("}},
                   RejectReason.INVALID_DEFINITION)
    assert service.storage.get_rule_version("bad", 1) is None
    assert service.storage.current_rules() == []
