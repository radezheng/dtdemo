# dtdemo (Microsoft Fabric Eventstream)

用 Python 模拟实体零售门店产生订单事件（JSON），发送到 Microsoft Fabric Eventstream 的 **Custom Endpoint**（Event Hubs 兼容连接串），并提供一个简单的消费端脚本用于打印/筛选事件。

## 功能

- `send_orders.py`：随机生成订单 JSON，并按随机间隔持续发送
- `receive_orders.py`：从 Event Hub 消费事件并打印 payload（支持按 `store_id` 过滤、限制最大条数）

## 前置条件

- Python **3.10+**（脚本使用了 `str | None` 这类类型标注）
- 可用的 Microsoft Fabric Eventstream Custom Endpoint 连接串（Event Hubs connection string）

## 安装

在仓库根目录执行：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install azure-eventhub
```

## 配置连接串（推荐使用 .env）

1) 复制模板：

```bash
cp .env.example .env
```

2) 编辑 `.env`，填入你的连接串（示例为占位符）：

```bash
EVENTHUB_SEND_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=..."
EVENTHUB_RECEIVE_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=..."
```

说明：

- `.env` 已在 `.gitignore` 中忽略，不会被提交（请不要把真实连接串写进代码/README）。
- 也可以只设置一个通用变量：`EVENTHUB_CONNECTION_STRING`（两端脚本都会用它作为兜底）。

## 运行

### 发送端：持续发送订单

```bash
python send_orders.py
```

常用参数：

- `--min-delay`：最小发送间隔（秒，默认 0.5）
- `--max-delay`：最大发送间隔（秒，默认 2.5）
- `--count`：发送条数（默认 0 表示无限循环）
- `--event-hub`：显式指定 event hub 名称（默认从连接串的 `EntityPath` 推断）
- `--connection-string`：直接传连接串（会覆盖 `.env`/环境变量）

示例（发送 10 条，间隔 0.2~1.0 秒）：

```bash
python send_orders.py --count 10 --min-delay 0.2 --max-delay 1.0
```

### 接收端：打印事件

```bash
python receive_orders.py
```

常用参数：

- `--consumer-group`：消费者组（默认 `$Default`）
- `--max-events`：最多接收多少条后退出（默认 0 表示持续运行）
- `--store-id`：按门店过滤（可重复指定多次）
- `--event-hub`：显式指定 event hub 名称（默认从连接串的 `EntityPath` 推断）
- `--connection-string`：直接传连接串（会覆盖 `.env`/环境变量）

示例（只看纽约门店，最多接收 20 条）：

```bash
python receive_orders.py --store-id STORE-NY-001 --max-events 20
```

## 环境变量优先级

两份脚本都支持 CLI、`.env` 和系统环境变量：

- `send_orders.py`：`--connection-string` > `EVENTHUB_SEND_CONNECTION_STRING` > `EVENTHUB_CONNECTION_STRING`
- `receive_orders.py`：`--connection-string` > `EVENTHUB_RECEIVE_CONNECTION_STRING` > `EVENTHUB_CONNECTION_STRING`

## 订单 JSON 示例

发送的 payload 大致结构如下（字段会随机变化）：

```json
{
  "order_id": "...",
  "order_timestamp": "2026-01-19T00:00:00.000000+00:00",
  "store_id": "STORE-SF-003",
  "store_region": "US-West",
  "store_city": "San Francisco",
  "items": [
    {
      "sku": "SKU-1002",
      "name": "27in Monitor",
      "quantity": 1,
      "unit_price": 115.17,
      "total_price": 115.17
    }
  ],
  "payment_method": "mobile_wallet",
  "order_total": 115.17,
  "loyalty_member": true
}
```

## 排错

- 报错缺少连接串：请检查 `.env` 是否存在、变量名是否正确，或使用 `--connection-string` 直接传入。
- `ValueError: Connection string does not contain an EntityPath segment`：你的连接串可能缺少 `EntityPath=...`。
- `azure.eventhub` 相关导入失败：确认已在虚拟环境中安装 `azure-eventhub`。
