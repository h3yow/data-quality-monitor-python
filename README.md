# data-quality-monitor (Python)

进程内存储模拟的本地数据质量校验与监控服务（FastAPI）。在**重复提交、并发执行、
规则变更**三种扰动下保证结论一致、失败原因可区分、历史可复现。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload
# 打开 http://127.0.0.1:8000/docs
```

## 示例流程

```bash
# 1. 登记 schema
curl -s -X POST localhost:8000/schemas -H 'Content-Type: application/json' -d '{
  "name": "orders",
  "fields": [
    {"name": "amount", "dtype": "float"},
    {"name": "status", "dtype": "str"}
  ]
}'

# 2. 登记规则（非法定义/字段缺失/规则冲突按错误码区分拒绝）
curl -s -X POST localhost:8000/rules -H 'Content-Type: application/json' -d '{
  "rule_id": "amt", "schema_name": "orders", "field_name": "amount",
  "rule_type": "RANGE", "severity": "ERROR", "params": {"min": 0, "max": 100}
}'

# 3. 登记批次（缺字段/类型不符按原因拒绝）
curl -s -X POST localhost:8000/batches -H 'Content-Type: application/json' -d '{
  "batch_id": "b1", "schema_name": "orders",
  "rows": [{"amount": 200.0, "status": "PAID"}]
}'

# 4. 幂等提交（重复/并发调用只有一次有效执行，accepted 区分）
curl -s -X POST localhost:8000/batches/b1/submit
# -> {"accepted": true, "execution": {"status": "SUCCEEDED", ...}}
curl -s -X POST localhost:8000/batches/b1/submit
# -> {"accepted": false, ...}  不重复执行、不重复计数

# 5. 结论与发现（按 ERROR/WARN/INFO 分类）
curl -s localhost:8000/batches/b1/conclusion
curl -s 'localhost:8000/batches/b1/findings?severity=ERROR'

# 6. 规则变更（新版本）后，历史批次仍按当时快照复现
curl -s -X POST localhost:8000/rules -H 'Content-Type:  application/json' -d '{
  "rule_id": "amt", "schema_name": "orders", "field_name": "amount",
  "rule_type": "RANGE", "severity": "ERROR", "params": {"min": 0, "max": 1000}
}'
curl -s -X POST localhost:8000/batches/b1/replay
# -> {"basis": "execution_rule_snapshot", "matches_historical": true, ...}
```

## 核心保证

- **登记强校验**：规则非法（类型/参数/类型兼容）、字段缺失、规则冲突（重复、
  合取为空、跨类型混用）全部携带可区分错误码拒绝，不静默跳过。
- **并发与幂等**：每批次一把锁 + 存储 CAS 占坑；重复/并发提交仅一次有效执行，
  结果与全局计数不重复；并发重试只有一个胜出。
- **无半完成结果**：findings 与计数随 SUCCEEDED 终态在同一锁内原子可见；
  中断置 FAILED 并清空结果，状态明确可恢复（retry 复用同一执行记录 attempt+1）。
- **版本化与留痕**：执行固化规则快照；规则发布新版本不影响历史结论，
  `replay` 按快照纯函数复现且 `matches_historical=true`。
- **查询一致**：findings 确定性排序，计数一次落定，重复查询恒定。

## 测试

```bash
uv run pytest -q     # 22 个用例，覆盖重复提交/并发/规则变更前后
uv run pytest -s     # 日志含每个场景的输入数据与判定依据
```

## 文档

- [docs/design.md](docs/design.md)：规则语义、状态流转、并发与幂等保证、
  接口清单与验证方法。
