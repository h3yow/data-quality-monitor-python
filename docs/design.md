# 本地数据质量校验与监控服务 — 设计说明

进程内（内置存储）实现的数据质量校验服务：登记 schema / 规则 / 批次，执行校验，
按严重级别分类失败与告警，并对规则版本化、对结果留痕。重点保证：**重复提交、
并发执行、规则变更三种扰动下结论一致且可解释**。

## 1. 模块结构

| 模块 | 职责 |
| --- | --- |
| `dq_monitor/models.py` | 枚举、错误码、领域模型（Schema / RuleVersion / Batch / Finding / Execution） |
| `dq_monitor/store.py` | 线程安全内置存储；CAS 状态流转；结果原子提交 |
| `dq_monitor/validators.py` | 字段定义校验、单规则合法性、规则间冲突检测（按原因区分拒绝） |
| `dq_monitor/engine.py` | 纯函数式规则求值、整批执行、中断/延迟测试钩子 |
| `dq_monitor/service.py` | 编排：登记、幂等提交、重试、结论、查询、快照复现 |
| `dq_monitor/api.py` | FastAPI HTTP 接口与错误码→HTTP 状态映射 |

## 2. 校验规则语义

### 2.1 规则类型

| 类型 | 参数 | 适用字段类型 | 语义 |
| --- | --- | --- | --- |
| `NOT_NULL` | 无 | 全部 | 值为 `null` 或键缺失即违规 |
| `RANGE` | `min` 和/或 `max` | `int` / `float` | `value < min` 或 `value > max` 违规，边界闭区间 |
| `ENUM` | `values`（非空列表） | `int` / `float` / `str` / `bool` | 取值不在集合内违规（类型严格，`1` 与 `true` 不等价） |
| `REGEX` | `pattern`（可编译正则） | `str` | `re.fullmatch` 不匹配即违规 |

- **空值归属**：`null` 只由 `NOT_NULL` 报告；其它取值规则对 `null` 跳过，
  避免一个空值产生多条噪声发现。
- 每条规则独立判定，一条数据可以命中多条规则、产生多条 finding；每条 finding
  都带 `rule_id + rule_version + row_index + value + reason`，可逐条解释。

### 2.2 严重级别与批次结论

| 级别 | 含义 | 对结论的影响 |
| --- | --- | --- |
| `ERROR` | 硬失败 | 存在任意 ERROR finding → 批次结论 `FAIL` |
| `WARN` | 告警 | 无 ERROR 但有 WARN → `WARN` |
| `INFO` | 提示 | 仅留痕与计数，不影响 PASS/FAIL |

结论优先级：`FAILED`（执行中断）/ `PENDING`（未执行或执行中）> `FAIL` > `WARN` > `PASS`。

### 2.3 登记时的强校验（非法即拒绝，绝不静默跳过）

错误原因通过错误码区分：

- 规则定义非法
  - `RULE_TYPE_INVALID`：规则类型不在四类之中
  - `RULE_PARAM_MISSING`：RANGE 无 min/max、ENUM 无 values、REGEX 无 pattern
  - `RULE_PARAM_INVALID`：边界颠倒、values 为空/重复/类型不符、正则不可编译、
    规则类型与字段类型不兼容（如字符串字段配 RANGE）、severity 非法
- 字段缺失 / 不存在
  - `FIELD_NOT_FOUND`：规则引用的字段不在 schema 中
  - `BATCH_FIELD_MISSING`：批次某行缺字段
  - `BATCH_TYPE_MISMATCH`：值与字段类型不符、`bool` 冒充 `int`、违反 `nullable=false`
- 规则互相冲突（`RULE_CONFLICT`，`details.reason` 进一步区分）：
  - `duplicate_range` / `duplicate_enum` / `duplicate_regex`：完全重复
    （重复定义会导致同一违例被重复计数，故拒绝）
  - `disjoint_ranges` / `disjoint_enums`：约束合取为空，任何取值都无法同时满足
  - `different_value_constraint_types`：同一字段同一级别混用 RANGE/ENUM/REGEX，
    交集无法一般化判定，保守拒绝；可改用不同严重级别分层表达
  - `no_change`：同 rule_id 提交与最新版本完全一致（应复用而非重建）
  - `rule_id_target_changed`：rule_id 试图改绑到其它 schema/字段
  - **允许**：部分重叠/包含关系的同类规则（每条独立判定，结论不矛盾）、
    NOT_NULL 与任意规则组合、跨严重级别的约束

## 3. 状态流转

### 3.1 规则版本

```
登记(rule_id 首次) -> ACTIVE v1
再次登记同 rule_id（内容有变化且未改绑）-> 旧版本置 SUPERSEDED，生成 ACTIVE v2、v3 ...
```

版本不可变；`GET /rules/{rule_id}` 返回完整版本链。

### 3.2 批次执行

```
                 submit(首次)
  (无执行记录) ───────────────▶ RUNNING ──执行成功──▶ SUCCEEDED（终态）
                                  │                    ▲
                                  │执行中断(无半成品)   │ retry / submit
                                  ▼                    │（原子覆盖空结果，attempt+1）
                                FAILED ────────────────┘
```

- 一个批次在存储中最多对应**一条**执行记录（重试复用同一 `execution_id`，
  仅 `attempt` 递增）。
- `SUCCEEDED` 为终态：重复提交直接返回，不重新执行；对终态 retry 返回冲突错误。
- `FAILED` 状态明确可恢复：findings/计数已清空（见 4.2），不存在"半完成"。
- 未提交时批次结论为 `PENDING`；执行中观察到的也是 `PENDING`。

## 4. 并发、幂等与一致性保证

### 4.1 同一批次并发/重复提交只有一次有效执行

1. 每个 `batch_id` 一把互斥锁（`service._lock_for`），同批次提交在服务层串行化；
2. 存储层 `create_running_if_absent` 用 CAS"占坑"：先到线程创建 RUNNING 记录，
   后到线程拿到同一条记录；
3. 执行中并发提交：观察到 `RUNNING`，返回同一记录（`accepted=false`），
   不重复执行；
4. 已成功再提交：返回终态（`accepted=false`），**计数、findings、stats 均不重复**；
5. 失败后提交/显式 retry：判定、CAS 与执行整体在批次锁内完成，并发重试
   串行化——只有一个线程看到 FAILED 并真正重试；胜者完成后其余线程读到
   SUCCEEDED 终态，收到 `RULE_CONFLICT(terminal_succeeded)`。

### 4.2 不留半完成结果（原子可见性）

- findings 只在执行函数的局部累积，执行期间外部不可见；
- 成功路径 `commit_execution` 在同一把存储锁内一次性写入
  **findings + 三类计数 + checked_rows + SUCCEEDED 时间戳**；
- 中断路径 `fail_execution` 清空所有结果字段并置 FAILED、记录 `fail_reason`；
- 因此任何线程、任何时刻读到的要么是"无结果"，要么是"完整结果"。

### 4.3 规则快照与历史复现

- 首次执行时把当时 ACTIVE 规则深拷贝为 `rule_snapshot`，与执行记录绑定；
- 执行只读快照，规则随后变更（v2/v3）不影响运行中与已完成的历史结论；
- `POST /batches/{id}/replay` 用快照对批次数据纯函数重算，返回
  `matches_historical`（必须为 `true`）；`rule_version` 参数可单独回放快照内
  某版本做对照。快照中不存在的新版本会被拒绝（新版本不追溯历史）。

### 4.4 查询稳定一致

- findings 在落库前按 `(rule_id, row_index, field_name)` 确定性排序；
- 所有读写均在存储锁保护下；过滤参数（severity / rule_id）不改变顺序；
- 计数在提交时一次性计算，重复查询结果恒定。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/schemas` | 登记 schema（201） |
| GET | `/schemas/{name}` | 查询 schema |
| POST | `/rules` | 登记规则 / 发布新版本（201） |
| GET | `/rules/{rule_id}` | 查询规则版本链 |
| POST | `/batches` | 登记批次并做完整性校验（201） |
| POST | `/batches/{id}/submit` | 幂等提交，返回 `{accepted, execution}` |
| POST | `/batches/{id}/retry` | 重试中断执行 |
| GET | `/batches/{id}/execution` | 执行记录（含快照与 findings） |
| GET | `/batches/{id}/conclusion` | 稳定结论与分类计数 |
| GET | `/batches/{id}/findings?severity=&rule_id=` | 确定性排序的发现列表 |
| POST | `/batches/{id}/replay?rule_version=` | 按历史快照复现 |
| GET | `/stats` | 全局计数 |
| GET | `/health` | 健康检查 |

错误响应统一形如：

```json
{"error": {"code": "RULE_CONFLICT", "message": "...", "details": {"reason": "disjoint_ranges"}}}
```

## 6. 验证方法

```bash
uv sync
uv run pytest -q                 # 全部用例（22 个）
uv run pytest -s                 # 查看输入数据与判定依据日志（dq.* logger）
uv run uvicorn main:app --reload # 启动服务，访问 /docs
```

测试覆盖：

- `tests/test_registration_validation.py`：非法定义、字段缺失、各冲突原因、
  批次完整性（原因可区分，日志打印输入与判定依据）
- `tests/test_execution_idempotency.py`：级别分类、5 次重复提交只执行一次、
  中断无半成品、retry 恢复、终态保护
- `tests/test_concurrency.py`：12 线程屏障并发提交、执行中并发提交、8 线程并发
  重试失败批次——均只有一次有效执行/一份结果/一次计数
- `tests/test_rule_versioning.py`：规则变更前后历史按 v1 复现、新旧批次结论
  对照、多次变更的完整版本链
- `tests/test_http_api.py`：错误码→HTTP 状态映射、接口幂等、HTTP 并发提交、
  中断重试接口

### 测试钩子（仅用于验证，数据行内元数据，不参与校验）

- `__crash__: "原因"`：执行到该行时模拟中断，验证 FAILED 可恢复、无半成品；
- `__delay__: 0.2`：该行前睡眠指定秒数，放大并发竞态窗口。
