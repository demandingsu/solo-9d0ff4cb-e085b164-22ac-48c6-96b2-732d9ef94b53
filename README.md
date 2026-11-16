# 雨量站批次存储 API（FastAPI + PyArrow + SQLite）

把雨量站频繁上传的小批 CSV 观测持久化为独立 Parquet 批次，按站日合并成
单个日文件，供暴雨复盘脚本以**快照一致**的方式读取。

- **Web 框架**：FastAPI（同步端点，跑在 threadpool 上，避免阻塞事件循环）
- **列存文件**：PyArrow / Parquet（zstd 压缩，UTC 毫秒时间戳）
- **元数据**：SQLite（WAL 模式，`BEGIN IMMEDIATE` 事务 + 进程内写锁）
- **交付**：可直接启动的 Docker Compose、本地样例 CSV、一键演示脚本、39 个测试
  （含换站边界、并发变更、快照一致与读取失败用例）

## 目录结构

```
app/
  config.py      环境变量与路径配置（RAIN_DATA_DIR 一处切换存储位置）
  db.py          SQLite 连接、WAL/忙等待、串行化写事务、表结构
  errors.py      领域异常 -> HTTP 状态码映射
  ingestion.py   CSV 解析/校验、规范 Parquet schema、记录去留（不去重、不排序）
  storage.py     Parquet 原子写、查询快照与引用计数、后台清理、崩溃恢复扫描
  services.py    上传 / 查询 / 合并 / 列表 的事务编排
  station_changes.py  换站登记（逻辑站映射）与跨换站逻辑站日查询
  main.py        FastAPI 路由与生命周期
samples/         本地雨量 CSV 样例（含中文表头样例与故意保留的重复行）
tests/           端到端与并发测试（pytest + TestClient + 线程池）
demo.sh          对运行中的服务执行 上传→查询→合并→冲突→清理 全流程
Dockerfile
docker-compose.yml
```

## 快速开始

### Docker Compose

```bash
docker compose up --build
# API: http://localhost:8000  交互式文档: http://localhost:8000/docs
```

数据保存在命名卷 `rain-data`（容器内 `/data`），含 SQLite 元数据库与
`parquet/<站号>/<日期>/*.parquet` 文件树。

### 本地直接运行（无需 Docker）

```bash
pip install -r requirements.txt
export RAIN_DATA_DIR=./data
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 一键演示

先启动服务，然后：

```bash
./demo.sh                                 # 默认 http://127.0.0.1:8000
RAIN_BASE_URL=http://其它主机:8000 ./demo.sh
```

### 跑测试

```bash
pip install -r requirements.txt pytest httpx
pytest -q
```

## API 入口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/stations/{station_id}/days/{day}/batches` | 上传单站单日 CSV（multipart `file`），作为独立批次保留，返回 `201` |
| `GET`  | `/stations/{station_id}/days/{day}/records` | **实际读取已发布 Parquet 文件**，返回全部记录与所用批次清单 |
| `POST` | `/stations/{station_id}/days/{day}/merge` | 把当前已发布的小批次原子合并为一个 Parquet 文件 |
| `GET`  | `/stations/{station_id}/days/{day}/batches` | 列出该站日全部批次（含 `published` / `superseded` 与文件是否还在） |
| `POST` | `/logical-stations/{logical_station_id}/days/{day}/changes` | **换站登记**：登记逻辑站、UTC 日期、旧站、新站与切换分钟（`201`） |
| `GET`  | `/logical-stations/{logical_station_id}/days/{day}/changes` | 读取该逻辑站日的换站登记 |
| `PUT`  | `/logical-stations/{logical_station_id}/days/{day}/changes` | 修改登记（部分字段）；冲突时 `409` 且原映射保持可用 |
| `GET`  | `/station-changes?day=YYYY-MM-DD` | 列出登记（可按 UTC 日期过滤） |
| `GET`  | `/logical-stations/{logical_station_id}/days/{day}/records` | **逻辑站日查询**：旧站只取切换前、新站只取切换后，保留重复行、批次号与实际站号 |
| `POST` | `/admin/reap` | 立即清理无查询引用的旧文件（后台线程默认每 5 秒自动执行） |
| `GET`  | `/health` | 健康检查 |

`day` 为 `YYYY-MM-DD`（UTC），`station_id` 为 1–64 位字母数字、`_`、`-`。
查询支持 `?limit=N`。

### CSV 格式

表头支持中英文别名，例如 `station_id,obs_time,rain_mm` 或
`站号,观测时间,雨量毫米`。要求：

- 每行站号与 URL 站号一致；
- 时间为 UTC，且必须落在 URL 指定的 UTC 日期内；
- 雨量为有限非负数值（毫米）；
- **完全保留重复行与原始顺序**，不做去重。

### curl 示例

```bash
curl -X POST -F "file=@samples/st001_2026-11-12_batch1.csv" \
  http://localhost:8000/stations/ST001/days/2026-11-12/batches

curl http://localhost:8000/stations/ST001/days/2026-11-12/records

curl -X POST http://localhost:8000/stations/ST001/days/2026-11-12/merge
```

## 换站登记与逻辑站日查询

雨量计中途更换时，实际站号会变化。登记一条映射后，即可按**同一逻辑站**
读取换站前后的原始观测。

```bash
# 1) 登记：LOG001 在 2026-11-12 由 ST001 换为 ST001R，12:00Z 起新站接管
curl -X POST http://localhost:8000/logical-stations/LOG001/days/2026-11-12/changes \
  -H 'Content-Type: application/json' \
  -d '{"old_station_id":"ST001","new_station_id":"ST001R","switch_minute":"2026-11-12T12:00Z"}'

# 2) 两个实际站各自照常上传（旧站/新站文件互不影响）
curl -X POST -F "file=@samples/st001_2026-11-12_preswitch.csv" \
  http://localhost:8000/stations/ST001/days/2026-11-12/batches
curl -X POST -F "file=@samples/st001r_2026-11-12_postswitch.csv" \
  http://localhost:8000/stations/ST001R/days/2026-11-12/batches

# 3) 逻辑站日查询
curl http://localhost:8000/logical-stations/LOG001/days/2026-11-12/records

# 4) 修改切换时刻（冲突时 409，原映射继续可用）
curl -X PUT http://localhost:8000/logical-stations/LOG001/days/2026-11-12/changes \
  -H 'Content-Type: application/json' \
  -d '{"switch_minute":"2026-11-12T08:00Z"}'
```

语义与约束：

- **切换边界**：旧站只取 `[00:00, switch_minute)`，新站只取
  `[switch_minute, 24:00)`；切换的那一分钟属于**新站**。`switch_minute`
  必须是该 UTC 日内整分钟（`YYYY-MM-DDTHH:MMZ`），且 `old ≠ new`。
- **记录保真**：每条记录保留 `station_id`（实际站号）、`segment`
  （`old`/`new`）、`source_batch_id` 与 `logical_station_id`；重复行原样保留，
  不排序去重，仅按观测时间稳定排序以便阅读。
- **物理站排他**：同一实际站同一天内不能被两个逻辑站登记到重叠时段。
  旧段 `[00:00,switch)`、新段 `[switch,24:00)` 均为左闭右开区间；创建或修改
  与之重叠的登记返回 `409 StationChangeConflictError`。
- **修改冲突不破坏现状**：冲突的 `PUT` 在单个 `BEGIN IMMEDIATE` 事务内回滚，
  原映射与 `version` 不变，登记查询与逻辑站查询继续可用。
- **快照一致**：查询在同一临界区内选取映射行与两站的已发布文件集合并完成引用
  计数 pin，上传、合并、登记修改都经同一进程内写锁串行化。因此映射与文件集合
  来自同一提交点；查询中途完成的合并不会改变本次结果，被 pin 的旧文件也不会被
  reaper 提前删除（须等查询结束，见 `tests/test_station_changes.py`）。
- **失败不返回部分数据**：两站所有文件全部读取成功后才组装结果；任一文件读取
  失败即整体返回 `500 QueryReadError`，不会返回已读到的前一部分记录。
- **原接口保持可用**：`/stations/{station_id}/days/{day}/records` 仍按实际站
  原样返回，不应用换站过滤。
- 查询同样支持 `?limit=N`；响应含 `total_row_count` 与 `truncated`。

## 并发与一致性设计（对应需求逐条）

1. **每次上传独立保留**：每次上传生成 `B-...` 批次，各自一个 Parquet 文件
   与一行元数据；查询结果里的 `source_batch_id` 保留每条记录的来源批次。
2. **查询真读 Parquet**：`GET .../records` 通过 `pyarrow.parquet.read_table`
   读取快照内每个文件并 `concat_tables` 拼接，不读缓存表。
3. **合并期间读写不中断**：合并在临时区构建新文件，完成前不触碰已发布集合；
   上传走独立文件 + 独立元数据行，全程可继续。
4. **只替换启动时选中的文件**：合并开始即固定源批次集合（写入
   `merge_locks` 并按 `rowid` 排序选取）；合并期间新上传的批次不在集合内，
   发布事务只 supersede 这批固定源，新批次继续保持 published。
5. **查询全程同一文件集合**：查询开始时选取已发布文件并对每个文件加引用计数，
   关闭快照前集合不变；即使合并在查询中途完成，查询仍读旧文件。
6. **旧文件等查询结束再清理**：合并只把旧批次标记 `superseded`，不立即删文件；
   后台 reaper（或 `/admin/reap`）仅删除引用计数为零的旧文件。
7. **同站日并发合并不重复纳入**：`merge_locks` 对 `(station_id, day)` 加主键行锁，
   第二个并发合并立即得到 `409 MergeConflictError`，不会把同一批数据纳入两次。
8. **合并失败不改已发布集合、半成品不可查**：合并文件先落临时/独立路径，
   元数据在单个 `BEGIN IMMEDIATE` 事务里完成“插入合并批次 + supersede 源”，
   任一步失败即回滚并删除半成品；半成品没有元数据行，永远不会出现在查询快照中。
   进程崩溃后重启时 `startup_sweep` 清理临时文件、无元数据的孤儿 Parquet 与残留锁。
9. **换站查询的单快照选取**：逻辑站查询在 `db.write_lock` 临界区内同时读取
   映射行与旧/新两站的已发布文件集合，并在离开临界区前完成引用 pin。上传、
   合并、登记创建/修改都取同一把锁，故三者可与查询并行而不会产生
   “新映射 + 旧文件集合”或“旧映射 + 新文件集合”的混合快照。
   `GET .../changes`、`PUT .../changes` 与 `/admin/reap` 均不需要查询等待。

### 边界说明

- 当前为**单容器/单进程**部署：引用计数与进程内写锁在进程内生效，SQLite 行锁
  提供持久化的合并互斥。多副本水平扩展需把文件存储换成 S3 之类共享存储、把
  引用计数与锁迁移到数据库/Redis，本仓库未包含该拓扑。
- 单文件上传大小受 `RAIN_MAX_UPLOAD_BYTES` 限制（默认 50 MiB）。

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RAIN_DATA_DIR` | `./data` | 数据根目录（SQLite、Parquet、临时文件） |
| `RAIN_DB_PATH` | `$RAIN_DATA_DIR/metadata.sqlite3` | SQLite 路径 |
| `RAIN_REAPER_INTERVAL` | `5` | 旧文件清理轮询间隔（秒） |
| `RAIN_MAX_UPLOAD_BYTES` | `52428800` | 单次上传字节上限 |
