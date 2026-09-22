"""登记与校验拒绝场景：规则非法、字段缺失、规则冲突必须可区分地拒绝。"""

from __future__ import annotations

import logging

import pytest

from dq_monitor.models import DQError, ErrorCode
from dq_monitor.service import DataQualityService

logger = logging.getLogger("dq.tests.registration")


def _setup(service: DataQualityService) -> None:
    service.register_schema(
        "orders",
        [
            {"name": "amount", "dtype": "float"},
            {"name": "status", "dtype": "str"},
            {"name": "qty", "dtype": "int", "nullable": False},
        ],
    )


# ------------------------------------------------------------------ #
# schema / 字段定义
# ------------------------------------------------------------------ #
def test_schema_fields_validated(service: DataQualityService) -> None:
    logger.info("输入: fields=%s", [{"name": "a", "dtype": "weird"}])
    with pytest.raises(DQError) as ei:
        service.register_schema("s1", [{"name": "a", "dtype": "weird"}])
    logger.info("判定依据: code=%s message=%s", ei.value.code, ei.value.message)
    assert ei.value.code is ErrorCode.RULE_PARAM_INVALID

    with pytest.raises(DQError) as ei:
        service.register_schema("s1", [{"name": "a", "dtype": "int"},
                                       {"name": "a", "dtype": "str"}])
    assert ei.value.code is ErrorCode.RULE_CONFLICT

    service.register_schema("s1", [{"name": "a", "dtype": "int"}])
    with pytest.raises(DQError) as ei:
        service.register_schema("s1", [{"name": "b", "dtype": "int"}])
    assert ei.value.code is ErrorCode.SCHEMA_EXISTS


# ------------------------------------------------------------------ #
# 单条规则：四种非法原因互相可区分
# ------------------------------------------------------------------ #
def test_rule_invalid_causes_are_distinguishable(service: DataQualityService) -> None:
    _setup(service)

    cases = [
        # (输入, 期望错误码, 判定依据)
        (("r", "orders", "missing_col", "RANGE", "ERROR", {"min": 0}),
         ErrorCode.FIELD_NOT_FOUND, "字段不存在"),
        (("r", "orders", "amount", "WEIRD", "ERROR", {}),
         ErrorCode.RULE_TYPE_INVALID, "规则类型非法"),
        (("r", "orders", "amount", "RANGE", "ERROR", {}),
         ErrorCode.RULE_PARAM_MISSING, "RANGE 缺 min/max"),
        (("r", "orders", "amount", "RANGE", "ERROR", {"min": 9, "max": 1}),
         ErrorCode.RULE_PARAM_INVALID, "RANGE 边界颠倒"),
        (("r", "orders", "amount", "ENUM", "ERROR", {}),
         ErrorCode.RULE_PARAM_MISSING, "ENUM 缺 values"),
        (("r", "orders", "amount", "ENUM", "ERROR", {"values": []}),
         ErrorCode.RULE_PARAM_INVALID, "ENUM 空列表"),
        (("r", "orders", "status", "REGEX", "ERROR", {}),
         ErrorCode.RULE_PARAM_MISSING, "REGEX 缺 pattern"),
        (("r", "orders", "status", "REGEX", "ERROR", {"pattern": "("}),
         ErrorCode.RULE_PARAM_INVALID, "REGEX 表达式非法"),
        (("r", "orders", "amount", "REGEX", "ERROR", {"pattern": ".*"}),
         ErrorCode.RULE_PARAM_INVALID, "数值字段不支持 REGEX"),
        (("r", "orders", "status", "RANGE", "ERROR", {"min": 1}),
         ErrorCode.RULE_PARAM_INVALID, "字符串字段不支持 RANGE"),
        (("r", "orders", "amount", "RANGE", "BOGUS", {"min": 0}),
         ErrorCode.RULE_PARAM_INVALID, "severity 非法"),
        (("r", "nope", "amount", "RANGE", "ERROR", {"min": 0}),
         ErrorCode.SCHEMA_NOT_FOUND, "schema 不存在"),
    ]
    seen: set[ErrorCode] = set()
    for args, expected, why in cases:
        rule_id, schema, field, rtype, sev, params = args
        logger.info(
            "输入: rule_id=%s schema=%s field=%s type=%s sev=%s params=%s",
            rule_id, schema, field, rtype, sev, params,
        )
        with pytest.raises(DQError) as ei:
            service.register_rule(rule_id, schema, field, rtype, sev, params)
        logger.info(
            "判定: code=%s（期望 %s）依据=%s | %s",
            ei.value.code, expected, why, ei.value.message,
        )
        assert ei.value.code is expected, (why, ei.value.code, expected)
        seen.add(expected)

    # 至少区分出字段缺失、类型非法、参数缺失、参数非法四类
    assert {
        ErrorCode.FIELD_NOT_FOUND,
        ErrorCode.RULE_TYPE_INVALID,
        ErrorCode.RULE_PARAM_MISSING,
        ErrorCode.RULE_PARAM_INVALID,
    }.issubset(seen)


# ------------------------------------------------------------------ #
# 规则冲突：不同原因
# ------------------------------------------------------------------ #
def test_rule_conflicts_rejected_with_reason(service: DataQualityService) -> None:
    _setup(service)
    service.register_rule("amt_rng", "orders", "amount", "RANGE",
                          "ERROR", {"min": 0, "max": 100})

    conflict_cases = [
        (("dup", "orders", "amount", "RANGE", "ERROR", {"min": 0, "max": 100}),
         "duplicate_range"),
        (("disjoint", "orders", "amount", "RANGE", "ERROR", {"min": 200, "max": 300}),
         "disjoint_ranges"),
        (("mixed", "orders", "amount", "ENUM", "ERROR", {"values": [1.0]}),
         "different_value_constraint_types"),
    ]
    for args, reason in conflict_cases:
        logger.info("冲突输入: %s，期望 reason=%s", args, reason)
        with pytest.raises(DQError) as ei:
            service.register_rule(*args)
        assert ei.value.code is ErrorCode.RULE_CONFLICT
        assert ei.value.details["reason"] == reason, ei.value.details
        logger.info("判定依据: code=RULE_CONFLICT reason=%s 与 %s 冲突",
                    reason, ei.value.details.get("conflict_with"))

    # 包含关系允许；不同严重级别允许共存；NOT_NULL 兼容
    service.register_rule("wide", "orders", "amount", "RANGE",
                          "ERROR", {"min": -1000, "max": 1000})
    service.register_rule("amt_warn", "orders", "amount", "RANGE",
                          "WARN", {"min": 50, "max": 60})
    service.register_rule("amt_nn", "orders", "amount", "NOT_NULL", "ERROR", {})
    assert len(service.store.active_rules_for_schema("orders")) == 4


def test_enum_conflict_reasons(service: DataQualityService) -> None:
    _setup(service)
    service.register_rule("st1", "orders", "status", "ENUM",
                          "ERROR", {"values": ["PAID", "DONE"]})
    for params, reason in [
        ({"values": ["PAID", "DONE"]}, "duplicate_enum"),
        ({"values": ["X"]}, "disjoint_enums"),
    ]:
        with pytest.raises(DQError) as ei:
            service.register_rule("st2", "orders", "status", "ENUM",
                                  "ERROR", params)
        assert ei.value.details["reason"] == reason


def test_identical_rule_resubmission_rejected(service: DataQualityService) -> None:
    _setup(service)
    service.register_rule("r1", "orders", "amount", "RANGE",
                          "ERROR", {"min": 0, "max": 100})
    with pytest.raises(DQError) as ei:
        service.register_rule("r1", "orders", "amount", "RANGE",
                              "ERROR", {"min": 0, "max": 100})
    assert ei.value.details["reason"] == "no_change"
    # rule_id 不能改绑
    with pytest.raises(DQError) as ei:
        service.register_rule("r1", "orders", "status", "REGEX",
                              "ERROR", {"pattern": ".*"})
    assert ei.value.details["reason"] == "rule_id_target_changed"


# ------------------------------------------------------------------ #
# 批次登记：字段缺失 / 类型不匹配 / 空批次 / 重复登记
# ------------------------------------------------------------------ #
def test_batch_registration_validation(service: DataQualityService) -> None:
    _setup(service)

    bad_batches = [
        ([], ErrorCode.BATCH_EMPTY, "空批次"),
        ([{"amount": 1.0, "status": "PAID"}],
         ErrorCode.BATCH_FIELD_MISSING, "缺 qty 字段"),
        ([{"amount": 1.0, "status": "PAID", "qty": None}],
         ErrorCode.BATCH_TYPE_MISMATCH, "qty 非空约束被违反"),
        ([{"amount": "abc", "status": "PAID", "qty": 1}],
         ErrorCode.BATCH_TYPE_MISMATCH, "amount 类型不匹配"),
        ([{"amount": 1.0, "status": "PAID", "qty": True}],
         ErrorCode.BATCH_TYPE_MISMATCH, "bool 不得冒充 int"),
    ]
    for rows, expected, why in bad_batches:
        logger.info("批次输入 rows=%s，依据=%s", rows, why)
        with pytest.raises(DQError) as ei:
            service.register_batch("bx", "orders", rows)
        assert ei.value.code is expected, (why, ei.value.code)
        logger.info("判定 code=%s row=%s field=%s",
                    ei.value.code,
                    ei.value.details.get("row_index"),
                    ei.value.details.get("field_name"))

    good = [{"amount": 1.0, "status": "PAID", "qty": 2}]
    service.register_batch("b1", "orders", good)
    with pytest.raises(DQError) as ei:
        service.register_batch("b1", "orders", good)
    assert ei.value.code is ErrorCode.BATCH_EXISTS
