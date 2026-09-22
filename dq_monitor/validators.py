"""规则参数校验与规则间冲突检测。

非法定义、字段缺失、规则冲突必须按原因区分并拒绝（抛 DQError），
不得静默跳过。

冲突判定原则（同 schema + 同字段 + 同严重级别）：
1. NOT_NULL 与任何规则兼容；跨级别规则允许共存（分层约束）。
2. 不同类型的取值约束（RANGE/ENUM/REGEX 混用）保守拒绝：
   交集无法一般化判定，避免语义含糊（reason=different_value_constraint_types）。
3. 同类规则：完全重复 -> 拒绝（否则每次违例会重复计数）；
   合取为空（区间/枚举互不相交）-> 拒绝（任何取值都无法同时满足）。
   部分重叠或包含关系允许共存：每条规则独立判定，结论不矛盾。
"""

from __future__ import annotations

import re

from .models import DQError, ErrorCode, RuleType, Schema

_VALID_DTYPES = {"int", "float", "str", "bool"}

# 各字段类型允许搭配的规则
_TYPE_COMPAT: dict[str, frozenset[RuleType]] = {
    "int": frozenset({RuleType.NOT_NULL, RuleType.RANGE, RuleType.ENUM}),
    "float": frozenset({RuleType.NOT_NULL, RuleType.RANGE, RuleType.ENUM}),
    "str": frozenset({RuleType.NOT_NULL, RuleType.ENUM, RuleType.REGEX}),
    "bool": frozenset({RuleType.NOT_NULL, RuleType.ENUM}),
}

_VALUE_RULES = frozenset({RuleType.RANGE, RuleType.ENUM, RuleType.REGEX})


# ------------------------------------------------------------------ #
# 字段定义
# ------------------------------------------------------------------ #
def validate_field_specs(fields: list[dict]) -> None:
    """校验字段定义本身：名称非空、类型受支持、不重复。"""
    if not fields:
        raise DQError(
            ErrorCode.RULE_PARAM_MISSING,
            "schema 至少需要一个字段",
            {"fields": fields},
        )
    seen: set[str] = set()
    for f in fields:
        name = f.get("name")
        dtype = f.get("dtype")
        if not name or not isinstance(name, str):
            raise DQError(
                ErrorCode.RULE_PARAM_MISSING,
                "字段名缺失或非法",
                {"field": f},
            )
        if name in seen:
            raise DQError(
                ErrorCode.RULE_CONFLICT,
                f"字段重复定义: {name}",
                {"field_name": name},
            )
        seen.add(name)
        if dtype not in _VALID_DTYPES:
            raise DQError(
                ErrorCode.RULE_PARAM_INVALID,
                f"字段 {name} 的类型 {dtype!r} 不受支持，允许: {sorted(_VALID_DTYPES)}",
                {"field_name": name, "dtype": dtype},
            )


def value_matches_dtype(value: object, dtype: str) -> bool:
    """JSON 取值是否与字段类型匹配。bool 不视作 int。"""
    if value is None:
        return True  # 是否允许为空由 NOT_NULL 规则判定
    if dtype == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if dtype == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if dtype == "str":
        return isinstance(value, str)
    if dtype == "bool":
        return isinstance(value, bool)
    return False


# ------------------------------------------------------------------ #
# 单条规则
# ------------------------------------------------------------------ #
def validate_rule_definition(
    rule_type: str,
    params: dict,
    schema: Schema,
    field_name: str,
) -> RuleType:
    """校验单条规则，返回归一化的 RuleType。

    依次区分：字段缺失 -> 规则类型非法 -> 参数缺失 -> 参数非法/类型不兼容。
    """
    field = schema.fields.get(field_name)
    if field is None:
        raise DQError(
            ErrorCode.FIELD_NOT_FOUND,
            f"schema {schema.name} 中不存在字段 {field_name}",
            {"schema_name": schema.name, "field_name": field_name},
        )

    try:
        rtype = RuleType(rule_type)
    except ValueError:
        raise DQError(
            ErrorCode.RULE_TYPE_INVALID,
            f"规则类型 {rule_type!r} 非法，允许: {[t.value for t in RuleType]}",
            {"rule_type": rule_type},
        ) from None

    if rtype not in _TYPE_COMPAT[field.dtype]:
        raise DQError(
            ErrorCode.RULE_PARAM_INVALID,
            f"字段 {field_name}({field.dtype}) 不支持规则类型 {rtype.value}",
            {
                "field_name": field_name,
                "dtype": field.dtype,
                "rule_type": rtype.value,
            },
        )

    if rtype is RuleType.NOT_NULL:
        return rtype  # 无参数

    if rtype is RuleType.RANGE:
        _validate_range(params, field.dtype)
    elif rtype is RuleType.ENUM:
        _validate_enum(params, field.dtype)
    elif rtype is RuleType.REGEX:
        _validate_regex(params)
    return rtype


def _validate_range(params: dict, dtype: str) -> None:
    if "min" not in params and "max" not in params:
        raise DQError(
            ErrorCode.RULE_PARAM_MISSING,
            "RANGE 规则必须提供 min 和/或 max",
            {"params": params},
        )
    lo, hi = params.get("min"), params.get("max")
    for name, v in (("min", lo), ("max", hi)):
        if v is not None and (
            isinstance(v, bool) or not isinstance(v, (int, float))
        ):
            raise DQError(
                ErrorCode.RULE_PARAM_INVALID,
                f"RANGE 参数 {name} 必须是数值，得到 {v!r}",
                {"param": name, "value": v},
            )
    if lo is not None and hi is not None and lo > hi:
        raise DQError(
            ErrorCode.RULE_PARAM_INVALID,
            f"RANGE 边界颠倒: min={lo} > max={hi}",
            {"min": lo, "max": hi},
        )


def _validate_enum(params: dict, dtype: str) -> None:
    values = params.get("values")
    if values is None:
        raise DQError(
            ErrorCode.RULE_PARAM_MISSING,
            "ENUM 规则必须提供非空 values 列表",
            {"params": params},
        )
    if not isinstance(values, list) or not values:
        raise DQError(
            ErrorCode.RULE_PARAM_INVALID,
            "ENUM 的 values 必须是非空列表",
            {"values": values},
        )
    for v in values:
        if not value_matches_dtype(v, dtype):
            raise DQError(
                ErrorCode.RULE_PARAM_INVALID,
                f"ENUM 取值 {v!r} 与字段类型 {dtype} 不匹配",
                {"value": v, "dtype": dtype},
            )
    if len({repr(v) for v in values}) != len(values):
        raise DQError(
            ErrorCode.RULE_PARAM_INVALID,
            "ENUM 的 values 存在重复元素",
            {"values": values},
        )


def _validate_regex(params: dict) -> None:
    pattern = params.get("pattern")
    if pattern is None:
        raise DQError(
            ErrorCode.RULE_PARAM_MISSING,
            "REGEX 规则必须提供 pattern",
            {"params": params},
        )
    if not isinstance(pattern, str) or not pattern:
        raise DQError(
            ErrorCode.RULE_PARAM_INVALID,
            "REGEX 的 pattern 必须是非空字符串",
            {"pattern": pattern},
        )
    try:
        re.compile(pattern)
    except re.error as exc:
        raise DQError(
            ErrorCode.RULE_PARAM_INVALID,
            f"REGEX 表达式非法: {exc}",
            {"pattern": pattern},
        ) from None


# ------------------------------------------------------------------ #
# 规则间冲突
# ------------------------------------------------------------------ #
def detect_conflicts(candidate: dict, existing: list[dict]) -> None:
    """与同一字段、同一严重级别的生效规则检测冲突。

    candidate / existing 元素结构:
      {rule_id, rule_type: RuleType, params: dict}
    冲突时抛 DQError(RULE_CONFLICT, ...)，details 指明与哪条规则、为何冲突。
    """
    c_type = candidate["rule_type"]
    if c_type not in _VALUE_RULES:
        return  # NOT_NULL 可与任意规则组合

    for other in existing:
        o_type = other["rule_type"]
        if o_type not in _VALUE_RULES:
            continue  # NOT_NULL 不构成冲突
        if c_type is not o_type:
            raise DQError(
                ErrorCode.RULE_CONFLICT,
                f"规则 {candidate['rule_id']}({c_type.value}) 与已有规则 "
                f"{other['rule_id']}({o_type.value}) 在同一字段/级别上混用了"
                "不同类型的取值约束，系统保守拒绝以免语义含糊，"
                "请使用相同约束类型或改用不同严重级别",
                {
                    "rule_id": candidate["rule_id"],
                    "conflict_with": other["rule_id"],
                    "reason": "different_value_constraint_types",
                    "field_name": candidate.get("field_name"),
                },
            )
        if c_type is RuleType.RANGE:
            _conflict_range(candidate, other)
        elif c_type is RuleType.ENUM:
            _conflict_enum(candidate, other)
        elif c_type is RuleType.REGEX:
            raise DQError(
                ErrorCode.RULE_CONFLICT,
                f"REGEX 规则 {candidate['rule_id']} 与 {other['rule_id']} "
                "在同一字段/级别上重复定义",
                {
                    "rule_id": candidate["rule_id"],
                    "conflict_with": other["rule_id"],
                    "reason": "duplicate_regex",
                },
            )


def _conflict_range(candidate: dict, other: dict) -> None:
    c_lo = candidate["params"].get("min", float("-inf"))
    c_hi = candidate["params"].get("max", float("inf"))
    o_lo = other["params"].get("min", float("-inf"))
    o_hi = other["params"].get("max", float("inf"))
    if c_lo == o_lo and c_hi == o_hi:
        reason = "duplicate_range"
        detail = "区间完全相同（重复定义）"
    elif c_hi < o_lo or o_hi < c_lo:
        reason = "disjoint_ranges"
        detail = "区间互不相交，任何取值都无法同时满足"
    else:
        return  # 部分重叠或包含：规则独立判定，结论不矛盾，允许共存
    raise DQError(
        ErrorCode.RULE_CONFLICT,
        f"RANGE 规则 {candidate['rule_id']} 与 {other['rule_id']} 冲突: {detail}",
        {
            "rule_id": candidate["rule_id"],
            "conflict_with": other["rule_id"],
            "reason": reason,
        },
    )


def _conflict_enum(candidate: dict, other: dict) -> None:
    c_set = {repr(v): v for v in candidate["params"]["values"]}
    o_set = {repr(v): v for v in other["params"]["values"]}
    c_keys, o_keys = set(c_set), set(o_set)
    if c_keys == o_keys:
        reason = "duplicate_enum"
        detail = "允许值集合完全相同（重复定义）"
    elif c_keys.isdisjoint(o_keys):
        reason = "disjoint_enums"
        detail = "允许值集合互不相交，任何取值都无法同时满足"
    else:
        return  # 部分重叠或包含：规则独立判定，结论不矛盾，允许共存
    raise DQError(
        ErrorCode.RULE_CONFLICT,
        f"ENUM 规则 {candidate['rule_id']} 与 {other['rule_id']} 冲突: {detail}",
        {
            "rule_id": candidate["rule_id"],
            "conflict_with": other["rule_id"],
            "reason": reason,
        },
    )
