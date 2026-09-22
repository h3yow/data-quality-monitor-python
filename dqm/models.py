"""核心数据模型：规则、批次、执行记录与校验发现。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


def _now() -> float:
    return time.time()


def _new_id() -> str:
    return uuid.uuid4().hex


class Severity(str, enum.Enum):
    """校验发现的严重级别。"""

    FAILURE = "failure"  # 硬失败：数据不可接受
    WARNING = "warning"  # 告警：数据可接受但需关注


class RuleType(str, enum.Enum):
    """支持的规则类型。"""

    NOT_NULL = "not_null"          # 字段不得为 null/缺失
    RANGE = "range"                # 数值字段须在 [min, max] 内
    REGEX = "regex"                # 字符串字段须匹配正则
    UNIQUE = "unique"              # 字段在批次内不得重复
    ENUM = "enum"                  # 字段取值须在允许集合内


class BatchStatus(str, enum.Enum):
    """批次状态机：REGISTERED -> RUNNING -> SUCCEEDED/FAILED；RUNNING 中断后恢复为 REGISTERED。"""

    REGISTERED = "registered"
    RUNNING = "running"
    SUCCEEDED = "succeeded"  # 执行完成且无 failure 级发现
    FAILED = "failed"        # 执行完成且存在 failure 级发现


class RejectReason(str, enum.Enum):
    """规则登记被拒绝的原因分类。"""

    INVALID_DEFINITION = "invalid_definition"  # 规则定义非法（如正则无法编译、min>max）
    MISSING_FIELD = "missing_field"            # 必填字段缺失
    RULE_CONFLICT = "rule_conflict"            # 与既有规则冲突


@dataclass(frozen=True)
class RuleSpec:
    """单条校验规则的声明式定义（输入）。"""

    rule_id: str
    rule_type: RuleType
    field: str
    severity: Severity = Severity.FAILURE
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuleVersion:
    """规则的一个不可变版本。规则变更产生新版本，历史版本保留以复现旧结论。"""

    version: int
    spec: RuleSpec
    created_at: float = field(default_factory=_now)


@dataclass
class Batch:
    """一个数据批次。"""

    batch_id: str
    rows: list[dict[str, Any]]
    status: BatchStatus = BatchStatus.REGISTERED
    registered_at: float = field(default_factory=_now)


@dataclass(frozen=True)
class Finding:
    """一条校验发现（命中某规则的某行/某值）。"""

    rule_id: str
    rule_version: int
    severity: Severity
    row_index: int | None  # UNIQUE 等批次级规则可为 None
    field: str
    message: str
    value: Any = None


@dataclass
class Execution:
    """一次校验执行。同一批次只允许多次尝试中的一次生效（幂等）。"""

    execution_id: str
    batch_id: str
    attempt: int
    rule_versions: dict[str, int]  # 执行时生效的规则快照: rule_id -> version
    status: str = "running"        # running / committed / aborted
    started_at: float = field(default_factory=_now)
    finished_at: float | None = None
    findings: list[Finding] = field(default_factory=list)


@dataclass(frozen=True)
class BatchReport:
    """批次的最终结论（查询视图，稳定一致）。"""

    batch_id: str
    status: BatchStatus
    execution_id: str | None
    rule_versions: dict[str, int]
    failures: tuple[Finding, ...]
    warnings: tuple[Finding, ...]

    @property
    def counts(self) -> dict[str, int]:
        return {"failures": len(self.failures), "warnings": len(self.warnings)}
