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


class TrialRejected(DQError):
    """影响试算被当场拒绝（规则非法/冲突），携带可区分原因，绝不给出半截结果。"""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")


class BackfillError(DQError):
    """回溯请求本身的问题（批次不存在、超过处理上界等）。"""


class WorkloadTooLarge(DQError):
    """批次数量等超过系统处理上界：明确拒绝，不截断、不给半截结果。"""
