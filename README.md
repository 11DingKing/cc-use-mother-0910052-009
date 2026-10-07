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

收盘后的权益、未实现盈亏与下一交易日可用额度以**唯一、可恢复的结算批次**为准，
不再依赖查询时的临时计算。结算表随启动自动建表（`settlement_batches` 等）。

### 口径区分（接口均带 `source`）

- `source=provisional`：盘中/盘后临时估值，按实时行情计算，只写估值审计日志，
  **永不产生分类账流水、永不封账**。
- `source=sealed`：已封账权威数字，附 `batch_no`、固定快照 `input_hash` 与分类账，
  封账后重发结算、迟到成交、重新估值都**不会改动或重复入账**。

### 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/trading/account/view?trade_date=YYYY-MM-DD` | 权益视图：已封账返回 sealed，否则 provisional |
| GET | `/api/trading/settlement/valuation` | 显式临时估值（不入账） |
| POST | `/api/trading/settlement/run` | 执行/续办日终结算（幂等，可断点续办） |
| GET | `/api/trading/settlement/{trade_date}` | 封账结果（权益、盈亏、费用、次日额度、来源） |
| GET | `/api/trading/settlement/{trade_date}/positions` | 封账跨日持仓快照 |
| GET | `/api/trading/settlement/{trade_date}/ledger` | 分类账流水（含幂等键） |
| GET | `/api/trading/settlements/history` | 历史结算批次 |
| POST | `/api/trading/late-trades` | 登记封账后迟到成交（挂起，次日恰好补入一次） |
| GET | `/api/trading/carry-forward` | 结转未完成订单与待补迟到成交 |
| POST | `/api/trading/settlement/restore` | 进程重启后从最近封账批次恢复账户内存状态 |

`settlement/run` 请求体：

```json
{
  "trade_date": "2026-10-07",
  "quotes": {"000001": 10.5},
  "corporate_actions": [
    {"type": "dividend", "stock_code": "000001", "cash_per_share": 0.5},
    {"type": "split", "stock_code": "000001", "ratio": 2}
  ],
  "fees": {}
}
```

### 可靠性保证

- **唯一批次**：`(账户, 交易日)` 数据库唯一约束，批次号 `STL_{账户}_{交易日}`；
  固定行情、订单、费用、企业行动快照与输入哈希随批次保存，可复现。
- **断点续办**：批次按 `init → ledger → positions → pending → sealed` 阶段推进，
  分类账流水按全局幂等键写入，持仓/结转订单整批替换；进程崩溃或重复触发安全续办。
- **不重复入账**：历史成交跨日重放时扣减更早封账批次已入账数量；
  迟到成交只挂起、次日以 `late_trade` 流水补入一次，绝不重开已封账批次。
- **顺序约束**：已有更晚封账批次时，拒绝乱序补做更早交易日（HTTP 409）。

