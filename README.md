# Data Quality Monitor

本地数据质量校验与监控服务。使用内置内存存储管理校验规则、数据批次与校验结果，
保证重复提交、并发执行与规则变更场景下结论一致且可解释。

## 结构

```
dqm/
  models.py    # 规则/批次/执行/发现/结论修订/试算结果/回溯作业的类型定义与状态枚举
  errors.py    # 领域错误（拒绝原因可区分）
  storage.py   # 内置存储：单锁临界区，执行权/回溯租约 CAS、结论修订账本、只读视图
  rules.py     # 规则登记：定义校验 + 冲突检测
  executor.py  # 纯函数求值 + 幂等执行（并发去重、中断恢复）
  trial.py     # 规则升级影响试算（纯只读、可重复、全有或全无）
  backfill.py  # 历史批次回溯（结论版本化、并发唯一生效、失败隔离、中断重试）
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
  同一份已提交结果——不重复执行、不重复计数。若抢到执行权的一方中途
  abort，等待方会重新争抢执行权自行补跑，收敛出唯一结论，
  不向调用方暴露"需重试"内部错误。
- **原子提交**：结果与批次状态推进在同一临界区完成，查询要么看到完整结论，
  要么看到未执行状态，绝不出现半完成结果。
- **规则快照**：执行开始时抓取当前生效规则版本快照（`rule_versions`），
  结论只与快照有关；之后规则变更不影响历史批次，可用快照重新求值复现。
- **查询稳定**：报告由已提交的不可变执行记录派生，重复查询结果一致。

## 规则升级影响试算（只读，发布前预演）

改规则前，先拿一条**待定规则定义**对着所有已登记历史批次跑一遍预演，
完全不触碰生效规则与历史结论：

- **纯只读**：试算只持有 `ReadOnlyStorageView`，物理上没有任何写接口可达；
  不升规则版本、不改批次结论、不新增执行记录。随便重复跑多少次，
  `TrialResult` 按值完全相等（候选版本时间戳也固定为哨兵值）。
- **判定口径**：试算快照 = 现行生效规则集 + 待定规则。待定 `rule_id` 与现行
  规则相同视为"升级候选"（替换现行版本，不与自身旧版本算冲突）；不同则作为
  "新增规则"叠加。每个批次的旧结论取其最近一次结论修订版本，因此试算结果与
  确认升级后回溯将产出的新结论严格对得上。
- **输出内容**：
  - `flipped_batch_ids`：结论翻盘的批次（succeeded↔failed；从未校验的批次
    无旧结论，翻盘标记为 false，但其新增命中仍会计入）；
  - `affected_batch_ids`：命中集合发生任何变化的批次；
  - 每个批次的 `added` / `removed`：比旧结论多出 / 少掉的具体命中；
  - `added_by_severity` / `removed_by_severity`：按 failure/warning 的整体分布。
- **全有或全无**：待定规则非法（`invalid_definition` / `missing_field`）或与
  现行规则打架（`rule_conflict`）当场拒绝（HTTP 422，`detail.reason` 区分），
  不静默跳过、不给半截结果；批次数量或单批行数超处理上界返回 413。
- **确定性**：批次按 `batch_id` 升序、规则按 `rule_id` 升序求值，命中顺序由
  `evaluate` 固定，与试算发起顺序、并发无关。

```python
from dqm.service import DQService
svc = DQService()
result = svc.trial_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                         "params": {"min": 0, "max": 120}})
print(result.flipped_batch_ids, result.added_by_severity)
for imp in result.impacts:
    print(imp.batch_id, imp.old_status, "->", imp.new_status,
          len(imp.added), len(imp.removed))
```

## 历史批次回溯（结论版本化，旧结论永不覆盖）

试算确认后，把挑出的一批历史批次整体按**当前生效规则**重跑：

- **结论修订版本（Revision）**：正常校验产生 v1（`source=normal`），
  每次真正回溯追加 v2/v3（`source=backfill`）。旧版本永久保留，
  可通过 `list_revisions` / `comparison` 并排追溯，绝不覆盖或抹掉。
- **新旧对照**：`comparison` 给出上一版与最新版的状态、是否翻盘
  （`flipped`）、`added` / `removed` 命中；报告 `GET /report` 的
  `revision_no` 标明当前生效的是第几版。
- **唯一生效（幂等 + 并发去重）**：回溯租约由存储层在**同一临界区**内完成
  "是否已最新 + 抢租约"判定（`acquire_rerun_if_stale`），同一批次被重复或
  并发回溯时只有一次真正执行，输的一方等待并复用同一结果，计数绝不翻倍。
  目标规则快照若已与最新结论一致，直接去重返回（`reran=false`），不产生新版本。
- **账本如实区分「真重跑」与「去重」**：作业账本里每个批次条目带 `reran`
  标记——`reran=true` 表示本次真正重跑并追加了一个新结论版本
  （`revision_no` 指向新版本）；`reran=false` 表示目标快照已与最新结论一致、
  本次被去重跳过（`revision_no` 指向既有版本，版本数不变）。两种情况在账本上
  一眼可分，不会混成同一种记录。
- **与正常校验互斥且结论对齐**：回溯租约占用期间，正常校验抢执行权会被拒
  （`try_begin_execution` 同样检查回溯租约），转为等待并复用回溯产出的结论；
  反过来正常校验在执行时回溯也会等待。**即使批次从未校验过**（只有登记数据），
  两边同时到达也只留一条生效结论：谁先占到执行权就由谁产出首版结论
  （`source` 记为 `backfill` 或 `normal`），另一边拿到同一份结果，
  绝不出现"正常校验一条、回溯一条"两条生效结论。
- **等待方自行收敛**：等待中的重复/并发调用如果遇到占用方中途 abort
  （中断/崩溃），不会被抛内部错误、批次也不会卡在未执行状态——等待方会
  重新争抢执行权自行补跑，最终收敛出唯一一份正确结论；中断方本身仍可
  安全重试。单个批次失败只影响自己，可单独重试到成功。
- **失败隔离 + 可单独重试**：批量回溯逐批次记账（`pending/running/
  succeeded/failed`）。某批次失败或被中断，只把该条目标记 `failed` 并记录
  错误、释放租约、恢复旧结论状态，不连累同批其它批次、不留半完成结论；
  可用作业级 `retry` 只重试未成功批次（成功的不重跑），也可对单批次重跑到成功。
- **有界处理，但结论等价**：单次作业批次上限 10000、并发度上限 8（默认 4）、
  单批行数上限 100000；超限明确拒绝而非截断。作业内条目按 `batch_id` 排序，
  最终结论与逐个串行处理完全一致，且与提交顺序无关。

```python
job = svc.backfill_batches(["b1", "b2"], max_workers=4)   # 批量回溯（作业账本）
svc.retry_backfill_job(job.job_id)                        # 只重试失败/未完成批次
cmp_res = svc.backfill_one("b1")                          # 单批回溯并直接拿对照
svc.list_revisions("b1")                                  # 全部历史结论版本
```

### 新旧结论兼容范围（规则变更会波及谁）

- **已按旧规则定论的批次**：结论默认保持不变（快照隔离），只有被显式纳入
  **回溯**的批次才会产生新版本；其旧版本始终可查。
- **规则变更后新登记/首次校验的批次**：直接适用新规则，不产生历史版本。
- **试算的影响面**：`affected_batch_ids` 即"若升级并回溯，命中会变"的批次；
  不在其中的批次（其数据对新旧规则表现一致，或字段与该规则无关）即使回溯，
  结论与命中也不变，只会因为快照版本不同而追加一版（可据 `comparison` 的
  `added/removed` 为空识别为"实质无影响"）。
- **从未校验的登记批次**：纳入试算（旧基线为空），也可直接回溯；其首版结论
  即按当前规则产生。

## HTTP 接口

```
# 原有
POST /rules                          登记/变更规则（422 时 detail.reason 区分拒绝原因）
POST /batches                        登记批次（幂等，返回 created）
POST /batches/{id}/validate          触发（或去重）校验，返回结论（413=超体量）
GET  /batches/{id}/report            查询结论（含 revision_no）
GET  /batches/{id}/status            查询批次状态

# 影响试算（只读）
POST /trials                         提交待定规则，返回翻盘/新增消失/级别分布
                                     422=非法或冲突，413=超处理上界

# 历史回溯
POST /backfills                      批量回溯 {batch_ids, max_workers?}，返回作业账本
GET  /backfills/{job_id}             查询作业逐批次状态与错误
POST /backfills/{job_id}/retry       只重试作业中未成功的批次
POST /batches/{id}/backfill          单独回溯一个批次并直接返回新旧对照
GET  /batches/{id}/comparison        最新两版结论并排对照
GET  /batches/{id}/revisions         列出全部历史结论版本（含 source）
```

## 验证方法

```bash
uv sync
uv run pytest -q        # 42 个测试，日志打印输入数据与判定依据
uv run uvicorn main:app # 启动 HTTP 服务（/docs 可交互试调）
```

测试覆盖：

- `tests/test_rules.py`：三类拒绝原因可区分；被拒绝规则不留残渣。
- `tests/test_execution.py`：重复提交去重；8 线程并发仅一次有效执行；
  中断恢复；并发中一次崩溃仍收敛到唯一生效执行；等待中的重复提交遇到
  占用方崩溃时自行补跑收敛（不抛内部错误、批次不卡住）。
- `tests/test_versioning.py`：规则变更后历史批次结论不变且可按快照复现；
  新批次适用新规则；重复查询稳定一致。
- `tests/test_trial.py`：翻盘/受影响批次、每批新增消失命中与级别分布；
  只读（规则版本、历史结论、版本数均不变）；重复试算按值相等且顺序确定；
  纳入未校验批次；非法/冲突/超量当场拒绝；试算仅持有只读视图（无写方法可达）。
- `tests/test_backfill.py`：回溯追加修订版本且旧结论保留可复现；新旧对照；
  快照已最新时去重不重跑；作业账本如实区分真重跑（`reran=true`）与去重
  （`reran=false`）；6 线程并发仅一次真正重跑（连跑稳定不闪烁）；
  中断后恢复旧结论、无半完成结果、可单独重试；批量失败隔离与作业重试；
  回溯与正常校验撞车结论一致；**未校验批次**上回溯与正常校验双向撞车
  只留一条生效结论；等待方遇占用方（正常校验/回溯）崩溃自行收敛；
  25 批次乱序 + 8 并发与逐个串行结论完全一致；批次数/单批行数超上界被拒绝。
- `tests/test_api.py`：试算只读/可重复端点、非法候选 422、回溯全流程与
  作业重试端点端到端可用。

### 本地手动走一遍（场景核对）

```bash
uv run uvicorn main:app --reload
# 另开终端：
curl -s -X POST localhost:8000/rules -H 'content-type: application/json' \
  -d '{"rule_id":"rg","rule_type":"range","field":"age","params":{"min":0,"max":200}}'
curl -s -X POST localhost:8000/batches -H 'content-type: application/json' \
  -d '{"batch_id":"b1","rows":[{"age":150},{"age":300}]}'
curl -s -X POST localhost:8000/batches/b1/validate        # 旧规则：1 条 failure

# ① 发布前只读试算：收紧到 [0,120]，看翻盘与新增/消失
curl -s -X POST localhost:8000/trials -H 'content-type: application/json' \
  -d '{"rule_id":"rg","rule_type":"range","field":"age","params":{"min":0,"max":120}}'
# 此时 GET /batches/b1/report 仍是旧结论 v1（试算未改动任何数据）

# ② 正式登记新规则，然后回溯历史批次
curl -s -X POST localhost:8000/rules -H 'content-type: application/json' \
  -d '{"rule_id":"rg","rule_type":"range","field":"age","params":{"min":0,"max":120}}'
curl -s -X POST localhost:8000/backfills -H 'content-type: application/json' \
  -d '{"batch_ids":["b1"]}'

# ③ 并排对照 + 追溯旧版本
curl -s localhost:8000/batches/b1/comparison             # v1 -> v2 差异
curl -s localhost:8000/batches/b1/revisions              # v1(normal)、v2(backfill) 都在
curl -s localhost:8000/batches/b1/report                 # 当前指向 v2
```

也可以不启服务，直接用一段 Python 走同一流程：

```bash
uv run python - <<'PY'
from dqm.service import DQService
svc = DQService()
svc.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                   "params": {"min": 0, "max": 200}})
svc.register_batch("b1", [{"age": 150}, {"age": 300}])
svc.validate_batch("b1")
trial = svc.trial_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                        "params": {"min": 0, "max": 120}})
print("试算翻盘:", trial.flipped_batch_ids, "新增分布:", trial.added_by_severity)
svc.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                   "params": {"min": 0, "max": 120}})
print("回溯对照:", svc.backfill_one("b1"))
print("历史版本:", [(r.revision_no, r.source, r.status.value)
                    for r in svc.list_revisions("b1")])
PY
```

### 亲自验证回溯记账与并发口径

三个关键场景都有稳定的自动化用例，单跑即可（日志会打印每个场景的依据与结论）：

```bash
# ① 账本区分真重跑 / 去重
uv run pytest tests/test_backfill.py::test_backfill_job_ledger_distinguishes_rerun_and_dedup -v

# ② 未校验批次上回溯与正常校验双向撞车，只留一条生效结论
uv run pytest tests/test_backfill.py::test_race_on_unvalidated_batch_backfill_first \
              tests/test_backfill.py::test_race_on_unvalidated_batch_validate_first -v

# ③ 等待方遇占用方中断后自行收敛（正常校验 / 回溯两个方向）
uv run pytest tests/test_execution.py::test_waiting_duplicate_converges_after_owner_crash \
              tests/test_backfill.py::test_backfill_waiter_converges_after_validate_crash \
              tests/test_backfill.py::test_validate_waiter_converges_after_backfill_crash -v
```

场景① 也可以用一段 Python 直接看账本：

```bash
uv run python - <<'PY'
from dqm.service import DQService
svc = DQService()
svc.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                   "params": {"min": 0, "max": 200}})
for bid in ("b1", "b2"):
    svc.register_batch(bid, [{"age": 150}, {"age": 300}])
    svc.validate_batch(bid)
svc.register_rule({"rule_id": "rg", "rule_type": "range", "field": "age",
                   "params": {"min": 0, "max": 120}})   # 收紧规则
svc.backfill_one("b1")                                  # b1 先回溯到 v2
job = svc.backfill_batches(["b1", "b2"])                # b1 已最新、b2 落后一版
for bid, item in job.items.items():
    print(bid, "reran =", item.reran, " revision =", item.revision_no)
# b1 reran = False  revision = 2   <- 去重：版本数不变
# b2 reran = True   revision = 2   <- 真重跑：本次追加的新版本
PY
```
