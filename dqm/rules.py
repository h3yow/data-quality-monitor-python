"""规则登记与校验：定义非法、字段缺失、规则冲突分别拒绝，绝不静默跳过。

校验顺序（原因可区分）：
1. validate_spec   —— 必填字段缺失 -> MISSING_FIELD；定义非法 -> INVALID_DEFINITION
2. check_conflicts —— 与既有生效规则冲突 -> RULE_CONFLICT
"""
from __future__ import annotations

import re

from dqm.errors import RuleRejected
from dqm.models import RejectReason, RuleSpec, RuleType, RuleVersion, Severity
from dqm.storage import Storage

# 每种规则类型要求的 params 键
_REQUIRED_PARAMS: dict[RuleType, tuple[str, ...]] = {
    RuleType.NOT_NULL: (),
    RuleType.RANGE: ("min", "max"),
    RuleType.REGEX: ("pattern",),
    RuleType.UNIQUE: (),
    RuleType.ENUM: ("allowed",),
}


def _missing(reason_detail: str) -> RuleRejected:
    return RuleRejected(RejectReason.MISSING_FIELD, reason_detail)


def _invalid(reason_detail: str) -> RuleRejected:
    return RuleRejected(RejectReason.INVALID_DEFINITION, reason_detail)


def validate_spec(spec: RuleSpec) -> None:
    """校验单条规则定义；非法时抛 RuleRejected(MISSING_FIELD/INVALID_DEFINITION)。"""
    if not spec.rule_id or not spec.rule_id.strip():
        raise _missing("rule_id 为空")
    if not spec.field or not spec.field.strip():
        raise _missing(f"规则 {spec.rule_id}: field 为空")
    if not isinstance(spec.rule_type, RuleType):
        raise _invalid(f"规则 {spec.rule_id}: 未知规则类型 {spec.rule_type!r}")
    if not isinstance(spec.severity, Severity):
        raise _invalid(f"规则 {spec.rule_id}: 未知严重级别 {spec.severity!r}")

    for key in _REQUIRED_PARAMS[spec.rule_type]:
        if key not in spec.params:
            raise _missing(f"规则 {spec.rule_id}: 缺少参数 {key!r}")

    if spec.rule_type is RuleType.RANGE:
        lo, hi = spec.params["min"], spec.params["max"]
        if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in (lo, hi)):
            raise _invalid(f"规则 {spec.rule_id}: min/max 必须为数值")
        if lo > hi:
            raise _invalid(f"规则 {spec.rule_id}: min({lo}) > max({hi})")
    elif spec.rule_type is RuleType.REGEX:
        pattern = spec.params["pattern"]
        if not isinstance(pattern, str):
            raise _invalid(f"规则 {spec.rule_id}: pattern 必须为字符串")
        try:
            re.compile(pattern)
        except re.error as exc:
            raise _invalid(f"规则 {spec.rule_id}: 正则无法编译: {exc}") from exc
    elif spec.rule_type is RuleType.ENUM:
        allowed = spec.params["allowed"]
        if not isinstance(allowed, (list, tuple, set)) or not allowed:
            raise _invalid(f"规则 {spec.rule_id}: allowed 必须为非空集合")


def check_conflicts(spec: RuleSpec, existing: list[RuleVersion]) -> None:
    """检查与既有生效规则的冲突；冲突时抛 RuleRejected(RULE_CONFLICT)。

    冲突定义（针对同一 field 的现行规则）：
    - 同一 field 已有相同类型的生效规则（重复定义，语义含糊）；
    - RANGE 与 ENUM 的取值域互不相交（任何数据都不可能同时满足）；
    - 两个 RANGE 区间互不相交。
    """
    for current in existing:
        other = current.spec
        if other.rule_id == spec.rule_id or other.field != spec.field:
            continue
        if other.rule_type is spec.rule_type:
            raise RuleRejected(
                RejectReason.RULE_CONFLICT,
                f"字段 {spec.field!r} 已存在同类型规则 {other.rule_id!r}"
                f"（v{current.version}），重复定义会导致结论含糊",
            )
        if spec.rule_type is RuleType.RANGE and other.rule_type is RuleType.RANGE:
            a = (spec.params["min"], spec.params["max"])
            b = (other.params["min"], other.params["max"])
            if a[1] < b[0] or b[1] < a[0]:
                raise RuleRejected(
                    RejectReason.RULE_CONFLICT,
                    f"字段 {spec.field!r} 的 RANGE 区间 {a} 与规则 {other.rule_id!r} "
                    f"的区间 {b} 互不相交，任何数据都无法同时满足",
                )
        pair = {spec.rule_type, other.rule_type}
        if pair == {RuleType.RANGE, RuleType.ENUM}:
            range_spec = spec if spec.rule_type is RuleType.RANGE else other
            enum_spec = other if spec.rule_type is RuleType.RANGE else spec
            lo, hi = range_spec.params["min"], range_spec.params["max"]
            numeric = [v for v in enum_spec.params["allowed"]
                       if isinstance(v, (int, float)) and not isinstance(v, bool)]
            if not any(lo <= v <= hi for v in numeric):
                raise RuleRejected(
                    RejectReason.RULE_CONFLICT,
                    f"字段 {spec.field!r} 的 ENUM 允许值与 RANGE [{lo}, {hi}] 互不相交",
                )


def register_rule(storage: Storage, spec: RuleSpec) -> RuleVersion:
    """登记规则（产生新版本），返回创建的版本。

    同一 rule_id 重复登记视为规则变更：重新走完整校验并递增版本，
    历史版本保留，供历史批次按当时生效规则复现结论。
    """
    validate_spec(spec)
    existing = storage.current_rules()
    check_conflicts(spec, existing)
    version = RuleVersion(version=storage.current_version(spec.rule_id) + 1, spec=spec)
    storage.append_rule_version(version)
    return version
