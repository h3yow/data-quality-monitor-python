"""新旧结论对照：命中配对、增删/翻盘判定与严重级别分布。

配对键为 (rule_id, row_index)：单条规则对同一行至多产生一条命中，
因此该键在一份发现中唯一，可稳定地把新旧命中一一配对。
"""
from __future__ import annotations

from dqm.models import (
    BatchImpact,
    BatchStatus,
    Finding,
    FindingChange,
    FindingDiff,
    Severity,
)

_FindingKey = tuple[str, int | None]


def _key(finding: Finding) -> _FindingKey:
    return finding.rule_id, finding.row_index


def _sort_key(change: FindingChange) -> tuple[str, int, int]:
    # None（批次级命中）排在最前；同一行再按变化类型排序
    row = change.row_index if change.row_index is not None else -1
    kind_order = {"removed": 0, "modified": 1, "added": 2}[change.kind]
    return change.rule_id, row, kind_order


def _to_change(kind: str, finding: Finding) -> FindingChange:
    return FindingChange(
        kind=kind,
        rule_id=finding.rule_id,
        rule_version=finding.rule_version,
        severity=finding.severity,
        row_index=finding.row_index,
        field=finding.field,
        message=finding.message,
        value=finding.value,
    )


def status_from_findings(findings: list[Finding]) -> BatchStatus:
    return (BatchStatus.FAILED
            if any(f.severity is Severity.FAILURE for f in findings)
            else BatchStatus.SUCCEEDED)


def diff_findings(old: list[Finding], new: list[Finding]) -> FindingDiff:
    """配对新旧两份发现，输出新增/消失/同点变化（确定性顺序）。

    - added/removed：只在一侧出现的命中；
    - modified：同一 (rule_id, row_index) 两侧都有，但严重级别或判定文案变化。
    """
    old_map = {_key(f): f for f in old}
    new_map = {_key(f): f for f in new}

    added = [_to_change("added", f) for key, f in new_map.items() if key not in old_map]
    removed = [_to_change("removed", f) for key, f in old_map.items() if key not in new_map]
    modified: list[FindingChange] = []
    for key, new_finding in new_map.items():
        old_finding = old_map.get(key)
        if old_finding is None:
            continue
        if (old_finding.severity != new_finding.severity
                or old_finding.message != new_finding.message):
            modified.append(_to_change("modified", new_finding))

    return FindingDiff(
        added=tuple(sorted(added, key=_sort_key)),
        removed=tuple(sorted(removed, key=_sort_key)),
        modified=tuple(sorted(modified, key=_sort_key)),
    )


def severity_deltas(old: list[Finding], new: list[Finding]) -> dict[str, int]:
    """按严重级别统计命中数净变化（新增-消失；级别翻转也自然计入）。"""
    deltas: dict[str, int] = {}
    for severity in Severity:
        old_count = sum(1 for f in old if f.severity is severity)
        new_count = sum(1 for f in new if f.severity is severity)
        deltas[severity.value] = new_count - old_count
    return deltas


def impact_for(batch_id: str, old_status: BatchStatus,
               old_findings: list[Finding], new_findings: list[Finding]) -> BatchImpact:
    """由新旧发现构造单个批次的影响结论（翻盘 + 严重级别净变化）。"""
    new_status = status_from_findings(new_findings)
    return BatchImpact(
        batch_id=batch_id,
        old_status=old_status,
        new_status=new_status,
        flipped=old_status is not new_status,
        diff=diff_findings(old_findings, new_findings),
        delta_by_severity=severity_deltas(old_findings, new_findings),
    )
