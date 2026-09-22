"""领域错误类型：所有拒绝都必须携带可区分的原因。"""
from __future__ import annotations

from dqm.models import RejectReason


class DQError(Exception):
    """服务层错误基类。"""


class RuleRejected(DQError):
    """规则登记被拒绝，reason 区分非法定义/字段缺失/规则冲突。"""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")


class BatchNotFound(DQError):
    pass


class BatchAlreadyExists(DQError):
    """同一 batch_id 重复登记。"""


class ExecutionConflict(DQError):
    """批次已有生效执行，重复/并发提交被拒绝或去重。"""
