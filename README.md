# 模拟日终结算业务服务

这是一个使用 Python、FastAPI 与 SQLite 实现的纯后端业务服务，包含领域模型、数据访问、业务编排、接口和异常路径测试。项目可在单个 Linux 应用容器内离线运行，使用本地 SQLite 或内存替身，不依赖外部运行服务。

## 安装

```bash
python3 -m pip install -r requirements.txt
```

## 测试

```bash
python3 -m pytest -q
```

## 构建检查

```bash
python3 -m compileall -q app
```

## API 导入冒烟

```bash
python3 -c "from app.main import app; print(len(app.routes))"
```

## 启动

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 日终结算

日终结算为模拟账户生成可恢复、可审计的唯一结算批次，重启、重复触发或中断后续办都不会重复入账。

### 设计要点

- **唯一批次**：同一账户同一交易日只有一个批次（数据库唯一约束），状态机为 `pending → sealing → sealed`，快照缺失落 `failed`，补齐收盘价后续办。
- **固定快照**：收盘价、费率、订单、企业行动在封账时定格；已封快照价格不可更改（冲突返回 409）。
- **确定性账本重放**：封账数字从上一已封账批次的持仓/现金出发，按订单过账台账增量重算，跨日未完成订单只过账新增成交量；封账结果整体替换，任意阶段崩溃后重跑结果一致。
- **顺序封账**：存在更早日期的未完成批次时必须先续办（409）。
- **封账后事件不回改历史**：
  - 迟到成交登记为 `late_fill/pending`，由下一交易日批次幂等消费；
  - 重新估值登记为 `restatement/rejected`，仅留痕、永不自动入账；
  - 对已封账日期重复触发结算返回同一批次。
- **来源区分**：接口以 `source=sealed_batch`（封账数字）或 `provisional`（临时估值）标注，并通过 `values_source`/`price_source` 标明数字来源。

### 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/trading/settlements/run` | 执行/续办日终结算（幂等） |
| GET | `/api/trading/settlements/{trade_date}` | 查询单日批次 |
| GET | `/api/trading/settlements` | 历史批次列表 |
| GET | `/api/trading/account-view/{trade_date}` | 权益视图（已封账优先，否则临时估值） |
| GET | `/api/trading/valuation` | 临时估值（不落库） |
| POST | `/api/trading/settlements/late-fills` | 登记封账后迟到成交 |
| POST | `/api/trading/settlements/restatements` | 登记重新估值（拒绝并留痕） |
| POST | `/api/trading/corporate-actions` | 登记企业行动（分红/送股/拆股） |
| GET | `/api/trading/post-seal-adjustments` | 查询封账后调整登记 |

