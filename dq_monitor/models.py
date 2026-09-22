"""核心领域模型与枚举（纯数据定义，不含业务逻辑）。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RuleType(str, Enum):
    NOT_NULL = "NOT_NULL"
    RANGE = "RANGE"
    ENUM = "ENUM"
    REGEX = "REGEX"


class Severity(str, Enum):
    ERROR = "ERROR"      # 硬失败：批次结论 FAIL
    WARN = "WARN"        # 告警：不致命，单独计数
    INFO = "INFO"        # 提示：仅留痕


class RuleStatus(str, Enum):
    ACTIVE = "ACTIVE"    # 当前生效版本
    SUPERSEDED = "SUPERSEDED"  # 被新版本取代（历史快照仍可复现）


class ExecutionStatus(str, Enum):
    RUNNING = "RUNNING"          # 执行中（占位，防止并发重复执行）
    SUCCEEDED = "SUCCEEDED"      # 成功完成（终态）
    FAILED = "FAILED"            # 执行中断（可重试，无半完成结果）


class ErrorCode(str, Enum):
    # 规则定义非法
    RULE_NOT_FOUND = "RULE_NOT_FOUND"
    RULE_TYPE_INVALID = "RULE_TYPE_INVALID"
    RULE_PARAM_MISSING = "RULE_PARAM_MISSING"
    RULE_PARAM_INVALID = "RULE_PARAM_INVALID"
    RULE_CONFLICT = "RULE_CONFLICT"
    # 模式/字段
    SCHEMA_NOT_FOUND = "SCHEMA_NOT_FOUND"
    SCHEMA_EXISTS = "SCHEMA_EXISTS"
    FIELD_NOT_FOUND = "FIELD_NOT_FOUND"
    # 批次
    BATCH_NOT_FOUND = "BATCH_NOT_FOUND"
    BATCH_EXISTS = "BATCH_EXISTS"
    BATCH_FIELD_MISSING = "BATCH_FIELD_MISSING"
    BATCH_TYPE_MISMATCH = "BATCH_TYPE_MISMATCH"
    BATCH_EMPTY = "BATCH_EMPTY"
    # 执行
    EXECUTION_NOT_FOUND = "EXECUTION_NOT_FOUND"
    EXECUTION_RUNNING = "EXECUTION_RUNNING"
    EXECUTION_CRASHED = "EXECUTION_CRASHED"


# 各错误码对应的 HTTP 状态码（api 层使用）
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.RULE_NOT_FOUND: 404,
    ErrorCode.RULE_TYPE_INVALID: 422,
    ErrorCode.RULE_PARAM_MISSING: 422,
    ErrorCode.RULE_PARAM_INVALID: 422,
    ErrorCode.RULE_CONFLICT: 409,
    ErrorCode.SCHEMA_NOT_FOUND: 404,
    ErrorCode.SCHEMA_EXISTS: 409,
    ErrorCode.FIELD_NOT_FOUND: 422,
    ErrorCode.BATCH_NOT_FOUND: 404,
    ErrorCode.BATCH_EXISTS: 409,
    ErrorCode.BATCH_FIELD_MISSING: 422,
    ErrorCode.BATCH_TYPE_MISMATCH: 422,
    ErrorCode.BATCH_EMPTY: 422,
    ErrorCode.EXECUTION_NOT_FOUND: 404,
    ErrorCode.EXECUTION_RUNNING: 409,
    ErrorCode.EXECUTION_CRASHED: 500,
}


class DQError(Exception):
    """业务错误：携带可区分的错误码与原因。"""

    def __init__(self, code: ErrorCode, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


@dataclass
class FieldSpec:
    name: str
    dtype: str          # int / float / str / bool
    nullable: bool = True


@dataclass
class Schema:
    name: str
    fields: dict[str, FieldSpec]
    created_at: datetime = field(default_factory=utcnow)


@dataclass
class RuleVersion:
    """规则的一个不可变版本。"""
    rule_id: str
    version: int
    schema_name: str
    field_name: str
    rule_type: RuleType
    severity: Severity
    params: dict
    status: RuleStatus
    created_at: datetime = field(default_factory=utcnow)

    def snapshot(self) -> dict:
        """执行时固化的快照（随结果留痕，用于复现）。"""
        return {
            "rule_id": self.rule_id,
            "version": self.version,
            "schema_name": self.schema_name,
            "field_name": self.field_name,
            "rule_type": self.rule_type.value,
            "severity": self.severity.value,
            "params": dict(self.params),
        }


@dataclass
class Batch:
    batch_id: str
    schema_name: str
    rows: list[dict]
    created_at: datetime = field(default_factory=utcnow)


@dataclass
class Finding:
    """单条校验发现（失败/告警/提示）。"""
    rule_id: str
    rule_version: int
    field_name: str
    rule_type: RuleType
    severity: Severity
    row_index: int
    value: object
    reason: str


@dataclass
class Execution:
    """一次校验执行。批次与执行一一对应，重复提交复用同一记录。"""
    execution_id: str
    batch_id: str
    status: ExecutionStatus
    rule_snapshot: list[dict]          # 执行时生效规则的不可变快照
    findings: list[Finding]            # 仅在完成（成功）时一次性可见
    error_count: int
    warn_count: int
    info_count: int
    checked_rows: int
    attempt: int                       # 第几次尝试（重试 +1）
    created_at: datetime = field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    fail_reason: str | None = None     # 中断原因（FAILED 时）


@dataclass
class Stats:
    schemas: int = 0
    rules: int = 0
    rule_versions: int = 0
    batches: int = 0
    executions: int = 0
    errors: int = 0
    warnings: int = 0
    infos: int = 0
