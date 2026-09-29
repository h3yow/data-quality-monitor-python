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


class BackfillState(str, enum.Enum):
    """单个批次的回溯结果。"""

    UPDATED = "updated"  # 已按当前生效规则产生新结论（旧结论留痕）
    CURRENT = "current"  # 本就按当前规则快照定论，无需重跑（幂等跳过）
    FAILED = "failed"    # 本次失败：旧结论仍生效，可单独重试


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
    # 瞬态同步原语（非业务数据）：执行提交/中止时置位，供并发去重方等待落定
    settled_event: Any = None


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


@dataclass(frozen=True)
class FindingChange:
    """单条命中的变化（按 rule_id + row_index 配对新旧命中）。"""

    kind: str              # "added" 新增命中 / "removed" 消失命中 / "modified" 同点变化（级别或文案）
    rule_id: str
    rule_version: int
    severity: Severity
    row_index: int | None
    field: str
    message: str
    value: Any = None


@dataclass(frozen=True)
class FindingDiff:
    """新旧两份发现的对照结果。"""

    added: tuple[FindingChange, ...] = ()
    removed: tuple[FindingChange, ...] = ()
    modified: tuple[FindingChange, ...] = ()

    def changed(self) -> bool:
        return bool(self.added or self.removed or self.modified)


@dataclass(frozen=True)
class BatchImpact:
    """试算/回溯中单个批次受规则变更的影响。"""

    batch_id: str
    old_status: BatchStatus
    new_status: BatchStatus
    flipped: bool
    diff: FindingDiff
    # 新结论相对旧结论的命中数净变化，按严重级别：{"failure": delta, "warning": delta}
    delta_by_severity: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class TrialReport:
    """待定规则的只读影响试算报告。试算绝不修改任何生效规则或历史结论。"""

    candidate_rule_id: str
    candidate_version: int                  # 假设发布后的版本号（现行版本 +1，新规则为 1）
    evaluated: tuple[BatchImpact, ...] = ()
    skipped: tuple[str, ...] = ()           # 尚无生效结论、未参与试算的批次
    rule_versions: dict[str, int] = field(default_factory=dict)  # 假设的生效规则快照

    @property
    def flipped_batches(self) -> tuple[str, ...]:
        return tuple(impact.batch_id for impact in self.evaluated if impact.flipped)

    @property
    def affected_batches(self) -> tuple[str, ...]:
        return tuple(impact.batch_id for impact in self.evaluated if impact.diff.changed())


@dataclass(frozen=True)
class BackfillItem:
    """批量回溯中单个批次的交代（成功/跳过/失败均可单独查询与重试）。"""

    batch_id: str
    state: BackfillState
    old_status: BatchStatus | None
    new_status: BatchStatus | None
    flipped: bool
    diff: FindingDiff
    new_execution_id: str | None = None
    error: str | None = None
    # 新结论相对旧结论的命中数净变化，按严重级别：{"failure": delta, "warning": delta}
    delta_by_severity: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class BackfillSummary:
    """一次批量回溯的汇总。逐个批次处理，单项失败不连累其它批次。"""

    items: tuple[BackfillItem, ...]
    # 严重级别净变化分布（仅 UPDATED 的批次）：{"failure": delta, "warning": delta}
    delta_by_severity: dict[str, int] = field(default_factory=dict)

    @property
    def updated(self) -> tuple[BackfillItem, ...]:
        return tuple(i for i in self.items if i.state is BackfillState.UPDATED)

    @property
    def failed(self) -> tuple[BackfillItem, ...]:
        return tuple(i for i in self.items if i.state is BackfillState.FAILED)
