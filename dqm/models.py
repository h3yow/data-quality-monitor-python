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
    revision_no: int | None = None  # 结论来自哪个修订版本（回溯后产生新版本）

    @property
    def counts(self) -> dict[str, int]:
        return {"failures": len(self.failures), "warnings": len(self.warnings)}


# ---------------------------------------------------------------------------
# 规则升级影响试算（只读）
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class BatchImpact:
    """待定规则对单个历史批次的影响（旧结论 vs 试算结论）。"""

    batch_id: str
    had_conclusion: bool                 # 该批次此前是否有已生效结论
    old_status: BatchStatus              # 无结论时为 REGISTERED
    new_status: BatchStatus
    flipped: bool                         # 结论是否翻盘（仅对有旧结论的批次有意义）
    old_rule_versions: dict[str, int]
    new_rule_versions: dict[str, int]
    old_counts: dict[str, int]
    new_counts: dict[str, int]
    added: tuple[Finding, ...]           # 比旧结论多出的命中
    removed: tuple[Finding, ...]         # 比旧结论少掉的命中


@dataclass(frozen=True)
class TrialResult:
    """一次影响试算的完整结果：纯只读派生，重复执行结果一致。"""

    candidate: RuleVersion                # 待定规则的假设版本（不入库、不生效）
    rule_versions: dict[str, int]         # 试算所用规则快照: rule_id -> version
    impacts: tuple[BatchImpact, ...]
    affected_batch_ids: tuple[str, ...]   # 命中集合发生变化的批次
    flipped_batch_ids: tuple[str, ...]    # 结论翻盘的批次
    added_by_severity: dict[str, int]     # 新增命中的严重级别分布
    removed_by_severity: dict[str, int]   # 消失命中的严重级别分布


# ---------------------------------------------------------------------------
# 历史批次回溯（结论版本化，旧结论永不覆盖）
# ---------------------------------------------------------------------------
class BackfillItemStatus(str, enum.Enum):
    """回溯作业中单个批次的状态。"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"       # 中断/异常：可单独重试，不连累其它批次


@dataclass(frozen=True)
class Revision:
    """批次的一个不可变结论修订版本；回溯追加新版本，旧版本永久保留。"""

    revision_no: int
    execution_id: str
    rule_versions: dict[str, int]
    status: BatchStatus
    findings: tuple[Finding, ...]
    source: str             # normal（正常触发）/ backfill（回溯）
    created_at: float = field(default_factory=_now)


@dataclass
class BackfillItem:
    """回溯作业中的单个批次条目。"""

    batch_id: str
    status: BackfillItemStatus = BackfillItemStatus.PENDING
    error: str | None = None
    revision_no: int | None = None      # 成功后批次的最新结论版本号
    reran: bool = False                 # 本次是否真正重跑（False=快照已最新，去重）


@dataclass
class BackfillJob:
    """一次批量回溯请求的状态账本：逐批次状态清楚、失败可重试。"""

    job_id: str
    items: dict[str, BackfillItem] = field(default_factory=dict)
    created_at: float = field(default_factory=_now)
    finished_at: float | None = None

    def items_by_status(self, status: BackfillItemStatus) -> list[BackfillItem]:
        return [item for item in self.items.values() if item.status is status]


@dataclass(frozen=True)
class RevisionComparison:
    """回溯新旧结论并排对照。"""

    batch_id: str
    previous_revision_no: int | None
    current_revision_no: int
    previous_status: BatchStatus | None
    current_status: BatchStatus
    flipped: bool
    reran: bool                         # False 表示当前规则快照与最新结论一致，未真正重跑
    added: tuple[Finding, ...]
    removed: tuple[Finding, ...]
