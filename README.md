# Data Quality Monitor

本地数据质量校验与监控服务。使用内置内存存储管理校验规则、数据批次与校验结果，
保证重复提交、并发执行与规则变更场景下结论一致且可解释。

## 结构

```
dqm/
  models.py    # 规则/批次/执行/发现/试算与回溯结果的类型定义与状态枚举
  errors.py    # 领域错误（拒绝原因可区分）
  storage.py   # 内置存储：单锁临界区，执行权 compare-and-set，结论谱系
  rules.py     # 规则登记：定义校验 + 冲突检测
  executor.py  # 纯函数求值 + 幂等执行（并发去重、中断恢复）
  diffing.py   # 新旧结论对照：命中配对、增删/翻盘、严重级别净变化
  trial.py     # 规则升级影响试算（只读、确定、可重复）
  backfill.py  # 历史批次回溯（重跑、留痕、并发去重、单项可重试）
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

## 规则升级影响试算（只读，发布前）

改完规则先别急着发：拿一条**待定规则定义**对着已登记的历史批次空跑一遍，
看清影响再决定。入口 `DQService.trial_rule(rule_payload, batch_ids=None)`
（HTTP `POST /rules/trial`）。

- **完全只读**：试算不创建执行、不改批次状态、不递增规则版本；候选规则只以
  临时对象参与内存求值，底层是 `executor.evaluate` 纯函数。随便重复跑多少次、
  与规则登记/批次校验怎么穿插，结果逐字节一致。
- **当场拒绝，不给半截结果**：候选规则先过完整的定义校验与冲突检测
  （`invalid_definition` / `missing_field` / `rule_conflict`）。冲突检测基于
  "假设该规则发布后"的规则集——同 `rule_id` 的旧版本视为被替换对象，
  不算自冲突；与其它现行规则打架则明确拒绝。任何拒绝都发生在求值之前。
- **报告内容**（`TrialReport`，逐批按 `batch_id` 排序，确定顺序）：
  - `flipped_batches`：结论翻盘（`succeeded` ⇄ `failed`）的批次；
  - 每个批次的 `diff`：`added`（新增命中）/ `removed`（消失命中）/
    `modified`（同一规则同一行的级别或文案变化），按 `(rule_id, row_index)`
    稳定配对；
  - `delta_by_severity`：命中数净变化按严重级别 `failure`/`warning` 分布；
  - `skipped`：尚无生效结论（只登记没跑过）的批次，明确列出而非悄悄漏掉。
- **处理上界**（超限整体拒绝并说明上界，绝不截断）：
  最多 500 个批次、单批最多 200,000 行（`dqm/trial.py` 常量可按需调整）。

```python
report = service.trial_rule(
    {"rule_id": "rg", "rule_type": "range", "field": "age",
     "params": {"min": 0, "max": 120}})
report.flipped_batches                 # ('b1', ...)
report.evaluated[0].diff.added         # 新增的命中（含规则、行号、文案、值）
report.evaluated[0].delta_by_severity  # {'failure': 1, 'warning': 0}
```

## 历史批次回溯（确认后整体重跑，旧结论留痕）

试算确认后，把挑出的历史批次整体按**当前生效规则**重跑。入口
`DQService.backfill(batch_ids)`（HTTP `POST /backfills`），单项可单独重试
`rerun_one(batch_id)`（HTTP `POST /batches/{id}/backfill`）。

- **旧结论绝不覆盖**：每次回溯提交只向批次的**结论谱系**追加一版
  （`conclusion_history`，HTTP `GET /batches/{id}/history`），最新一条即当前
  结论，历史各版随时可查；`compare_conclusions`
  （HTTP `GET /batches/{id}/compare`）并排对照最新两版的翻盘/新增/消失。
- **每项交代清楚、失败可单独重试**：逐批串行处理，单项结果为
  `updated`（产生了新结论）/ `current`（本就按当前规则定论，幂等跳过）/
  `failed`（本次失败、原因写明）。一个批次失败或中断只影响它自己：
  旧结论仍生效、无半完成结果、谱系不追加，其它批次照常更新；对 `failed`
  项再次调用即可重试到成功。尚无任何生效结论的批次不是可回溯对象，
  对应项返回 `failed` 并说明原因，不连累同批其它项。
- **并发唯一生效、计数不翻倍**：回溯重跑与正常校验共用同一道执行权
  compare-and-set（进行中 + 已提交两层）。同一批次无论被重复/并发地从回溯还是
  正常触发丢进来，同一时刻只有一次真正执行，其余等待并复用同一份结论。
- **顺序无关**：入参去重后按 `batch_id` 排序串行处理，单批结论只取决于自身数据
  与当前规则快照，因此先回溯谁、后回溯谁，或回溯正好撞上正常校验，最终结论与
  "逐个处理"完全一致。
- **处理上界**：最多 500 个批次、单批最多 2,000,000 行；超限整体拒绝，
  不留半截结果（`dqm/backfill.py` 常量可调整）。

### 新旧结论的兼容范围（哪些批次会被波及）

- **会被规则变更波及**的，是结论所依据的规则快照与当前生效快照**不一致**的已
  执行批次：试算会把它们的差异算出来；回溯时这些批次会 `updated`。
- **不受影响**的：① 结论所依据的规则快照与当前一致的批次，回溯时判为
  `current`，不产生新版本、计数不变；② 只登记、从未执行因而没有生效结论的
  批次，试算列入 `skipped`、回溯列为 `failed`（需先走一次正常校验）；
  ③ 与变更字段/规则无关、求值结果不变的批次，会出现在报告里但 `diff` 为空、
  `flipped=False`——明确告诉你"看过了，没影响"。
- 规则版本只增不改：历史版本永久保留，任一版结论都可用其 `rule_versions`
  快照重新求值复现。

## HTTP 接口

```
POST /rules                      登记/变更规则（422 时 detail.reason 区分拒绝原因）
POST /rules/trial                待定规则只读影响试算（422 非法/冲突，413 超规模，404 批次不存在）
POST /batches                    登记批次（幂等，返回 created）
POST /batches/{id}/validate      触发（或去重）校验，返回结论
GET  /batches/{id}/report        查询结论（稳定一致）
GET  /batches/{id}/status        查询批次状态
POST /backfills                  批量回溯（body: {"batch_ids": [...]}），返回逐项交代
POST /batches/{id}/backfill      单独回溯/重试一个历史批次
GET  /batches/{id}/history       查询批次的全部历史结论（旧结论留痕）
GET  /batches/{id}/compare       并排对照最新两版结论（翻盘/新增/消失）
```

## 验证方法

```bash
uv sync
uv run pytest -q        # 24 个测试，日志打印输入数据与判定依据
uv run uvicorn main:app # 启动 HTTP 服务
```

测试覆盖：

- `tests/test_rules.py`：三类拒绝原因可区分；被拒绝规则不留残渣。
- `tests/test_execution.py`：重复提交去重（单次生效、计数不重复）；
  8 线程并发仅一次有效执行且结论一致；中断后状态恢复 `REGISTERED`、
  无半完成结果、重试成功；并发中一次崩溃仍收敛到唯一生效执行。
- `tests/test_versioning.py`：规则变更后历史批次结论不变且可按当时
  快照复现；新批次适用新规则；失败/告警按严重级别分类；重复查询稳定一致。
- `tests/test_trial.py`：试算只读且重复运行逐字一致、不改规则版本与历史结论；
  翻盘批次、新增/消失命中、严重级别净变化准确；新规则/变更规则两种候选；
  非法定义、缺字段、规则冲突均在求值前当场拒绝；超规模整体拒绝。
- `tests/test_backfill.py`：回溯前后新旧结论并排对照、三版谱系全部可查；
  succeeded⇄failed 翻盘；重复/8 线程并发回溯只有一次生效且计数不翻倍；
  单批中断被标记 `failed`、旧结论仍生效、不连累其它批次、单独重试成功；
  回溯顺序不同最终结论与谱系一致；回溯与正常校验撞车收敛到同一结论；
  未执行批次明确判为不可回溯；直接重跑中断后执行权释放并可恢复。

### 本地手动走一遍

```bash
uv run uvicorn main:app &
# 1) 登记规则与历史批次，跑出新规则前的结论
curl -s -XPOST localhost:8000/rules -H 'content-type: application/json' \
  -d '{"rule_id":"rg","rule_type":"range","field":"age","params":{"min":0,"max":200}}'
curl -s -XPOST localhost:8000/batches -H 'content-type: application/json' \
  -d '{"batch_id":"b1","rows":[{"age":150},{"age":30}]}'
curl -s -XPOST localhost:8000/batches/b1/validate
# 2) 发布前只读试算：看哪些批次翻盘、新增哪些命中（规则与历史结论不变）
curl -s -XPOST localhost:8000/rules/trial -H 'content-type: application/json' \
  -d '{"rule":{"rule_id":"rg","rule_type":"range","field":"age","params":{"min":0,"max":120}}}'
# 3) 发布新规则，再整体回溯历史批次
curl -s -XPOST localhost:8000/rules -H 'content-type: application/json' \
  -d '{"rule_id":"rg","rule_type":"range","field":"age","params":{"min":0,"max":120}}'
curl -s -XPOST localhost:8000/backfills -H 'content-type: application/json' \
  -d '{"batch_ids":["b1"]}'
# 4) 查旧结论谱系、并排对照；重复回溯只会得到 current（幂等）
curl -s localhost:8000/batches/b1/history
curl -s localhost:8000/batches/b1/compare
curl -s -XPOST localhost:8000/backfills -H 'content-type: application/json' \
  -d '{"batch_ids":["b1"]}'
```
