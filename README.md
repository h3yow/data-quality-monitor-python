# Data Quality Monitor

本地数据质量校验与监控服务。使用内置内存存储管理校验规则、数据批次与校验结果，
保证重复提交、并发执行与规则变更场景下结论一致且可解释。

## 结构

```
dqm/
  models.py    # 规则/批次/执行/发现的类型定义与状态枚举
  errors.py    # 领域错误（拒绝原因可区分）
  storage.py   # 内置存储：单锁临界区，执行权 compare-and-set
  rules.py     # 规则登记：定义校验 + 冲突检测
  executor.py  # 纯函数求值 + 幂等执行（并发去重、中断恢复）
  service.py   # 服务门面
main.py        # FastAPI 薄接口层
tests/         # pytest 测试（日志打印输入数据与判定依据）
```

## 校验规则语义

规则为声明式定义：`rule_id`、`rule_type`、`field`、`severity`、`params`。

| 类型 | params | 语义 |
|---|---|---|
| `not_null` | — | 字段不得缺失或为 null |
| `range` | `min`, `max` | 数值字段须在闭区间内；非数值记为发现 |
| `regex` | `pattern` | 字符串字段须整体匹配正则 |
| `enum` | `allowed` | 取值须在允许集合内 |
| `unique` | — | 字段值在批次内不得重复 |

- 字段缺失/`null` 统一由 `not_null` 规则报告，其余规则跳过 `null`，避免重复计数。
- 严重级别：`failure`（硬失败）与 `warning`（告警）。批次存在任一 `failure`
  级发现则结论为 `failed`，否则为 `succeeded`。

### 规则登记拒绝（不静默跳过，原因可区分）

登记时依次校验，任一不满足即以对应原因拒绝，且不留下部分登记的残渣：

| 原因 | 触发条件 |
|---|---|
| `missing_field` | 缺 `rule_id`/`field`，或缺类型要求的 params（如 `pattern`、`max`） |
| `invalid_definition` | 未知类型/级别、`min > max`、正则无法编译、空允许集合 |
| `rule_conflict` | 同字段同类型重复定义；RANGE 区间互不相交；ENUM 与 RANGE 取值域互不相交 |

同一 `rule_id` 再次登记视为**规则变更**：重新走完整校验并递增版本，
历史版本保留，供历史批次复现结论。

## 状态流转

批次状态机：

```
REGISTERED --(获得执行权)--> RUNNING --(提交结果)--> SUCCEEDED / FAILED
               ^                 |
               |                 | (执行中断 abort)
               +--- 恢复 ---------+
```

- 执行中断（异常/崩溃）时 `abort`：释放执行权、批次回到 `REGISTERED`、
  不保留任何半完成结果，可安全重试。
- 执行记录状态：`running -> committed | aborted`。

## 并发与幂等保证

- **批次登记幂等**：同一 `batch_id` 重复登记返回 `created=false`，不覆盖原数据。
- **执行唯一生效**：`try_begin_execution` 在单锁内做 compare-and-set，
  同一批次并发/重复提交时只有一次执行获得生效权；其余调用等待并返回
  同一份已提交结果——不重复执行、不重复计数。
- **原子提交**：结果与批次状态推进在同一临界区完成，查询要么看到完整结论，
  要么看到未执行状态，绝不出现半完成结果。
- **规则快照**：执行开始时抓取当前生效规则版本快照（`rule_versions`），
  结论只与快照有关；之后规则变更不影响历史批次，可用快照重新求值复现。
- **查询稳定**：报告由已提交的不可变执行记录派生，重复查询结果一致。

## HTTP 接口

```
POST /rules                      登记/变更规则（422 时 detail.reason 区分拒绝原因）
POST /batches                    登记批次（幂等，返回 created）
POST /batches/{id}/validate      触发（或去重）校验，返回结论
GET  /batches/{id}/report        查询结论（稳定一致）
GET  /batches/{id}/status        查询批次状态
```

## 验证方法

```bash
uv sync
uv run pytest -q        # 10 个测试，日志打印输入数据与判定依据
uv run uvicorn main:app # 启动 HTTP 服务
```

测试覆盖：

- `tests/test_rules.py`：三类拒绝原因可区分；被拒绝规则不留残渣。
- `tests/test_execution.py`：重复提交去重（单次生效、计数不重复）；
  8 线程并发仅一次有效执行且结论一致；中断后状态恢复 `REGISTERED`、
  无半完成结果、重试成功；并发中一次崩溃仍收敛到唯一生效执行。
- `tests/test_versioning.py`：规则变更后历史批次结论不变且可按当时
  快照复现；新批次适用新规则；失败/告警按严重级别分类；重复查询稳定一致。
