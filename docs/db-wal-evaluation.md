# SQLite WAL 评估（Phase 2）

> 本文区分三种证据：`[测]` = 本机实测（2026-11-19，Linux 容器 overlayfs + go1.26.6 +
> modernc sqlite 1.38，**没有生产库副本、没有生产磁盘**）；`[码]` = 读源码得到；
> `[推]` = 推理，未在本环境验证。生产形态（docker + 真实 7 GiB 库）的结论只能由真机给出，
> 清单见第七节。

---

## 一、结论

| 维度 | 结论 |
|---|---|
| 对网关核心路径的价值 | **高**。它治的是"读挡写"这一类故障，而那正是状态页轮询、统计长读、迁移/回收期间聊天请求报 `SQLITE_BUSY` 的根因（见 `docs/db-compression-phase6.md` 的真机 5.018 秒） |
| 本机提交吞吐 | `[测]` 2000 次 1 KB 自动提交：delete+FULL **19.7 s** → WAL+NORMAL **193 ms**（约 100×，但见第三节的口径说明） |
| 本机读挡写 | `[测]` 长读事务持锁时写入：delete 等满 `busy_timeout` 后失败（201 ms），WAL **50 µs 成功** |
| 风险 | 集中且**已在本仓库内收口**：备份口径、文件大小口径、`-wal` 生命周期；外部风险是网络文件系统与断电丢最后几笔 |
| 建议 | **默认仍关闭，作为可选开关 `DB_WAL` 发布**。默认切换需要真机清单全绿后再单独决策 |

所以本次交付的是：**一个默认关闭的开关 + 两条被修正的口径 + 一组能复现的证据**，
不是一次默认行为变更。

---

## 二、问题：delete 模式下"读挡写"是结构性代价

`[码]` 当前 DSN 只有 `_txlock=immediate`（`models/init.go`）。它解决的是另一件事：
deferred 事务升级写锁时互等会**直接**返回 `SQLITE_BUSY`（不走 `busy_timeout`），
并发写者成片失败。`BEGIN IMMEDIATE` 把写锁提前到事务开头，让写者改由 `busy_timeout` 排队。

但排队仍然要排。`journal_mode=delete` 下：

- 写事务提交需要 **EXCLUSIVE** 锁，而任何持着 SHARED 的读者（**一次普通查询就算**）
  都挡着它；
- 于是写者等满 `busy_timeout`（驱动默认 5000 ms）后失败；
- 长读反过来也一样：状态页每 2 秒轮询一次的全表聚合，会把这段时间里的日志写入全部挡住。

`docs/db-compression-phase6.md` 记录过真机现场：一次 4.24 秒的长读，让聊天请求等满 5 秒
报 500。清理/迁移/回收三条周期任务的批次大小（`nextBatchSize`）也都是按这个 5 秒预算
算出来的——**整个系统的并发模型建立在"写者迟早能拿到锁"这个假设上，而这个假设在
delete 模式下随时会被一次读打破。**

WAL 改的正是这条假设：读者读的是 WAL 里的快照，写者往 WAL 尾部追加，两者不互斥。

---

## 三、本机实测

### 3.1 长读持锁时写入 `[测]`

场景：一个连接开事务、`SELECT` 后**不关结果集**（模拟一次长读），另一个连接做一次
自动提交的 `INSERT`，`busy_timeout=200 ms`。

| 模式 | 写入结果 | 耗时 |
|---|---|---|
| delete | `SQLITE_BUSY` 失败 | 201.6 ms（等满超时） |
| WAL | 成功 | **49.8 µs** |

对应用例：`models/wal_test.go` 的 `TestWALReadTransactionDoesNotBlockWriter`（两种模式都断言，
不是只测 WAL）。它同时钉住一个容易踩错的细节：**"读连接"不能带 `_txlock=immediate`**，
否则 `BEGIN` 会去抢写锁，读者变成另一个写者，这条用例就验不到"读不挡写"了。

生产里真正走这条路的读者是单条查询与 `Raw().Row()`——GORM 不为单条查询开事务，
驱动不上 `BEGIN`，所以 `_txlock` 对它们不生效。

### 3.2 提交吞吐 `[测]`

2000 次 1 KB 行的自动提交（每次提交一条语句，最接近"每个请求写一行日志"的形态）：

| 组合 | 总耗时 | 速率 | 主库 | `-wal` | 合计 |
|---|---|---|---|---|---|
| delete + FULL（当前默认） | 19.66 s | 102 次/秒 | 2.7 MB | — | 2.7 MB |
| delete + NORMAL | 12.98 s | 154 次/秒 | 2.7 MB | — | 2.7 MB |
| **WAL + NORMAL（本开关）** | **193 ms** | **10 355 次/秒** | 2.5 MB | 4.1 MB | 6.6 MB |
| WAL + FULL | 7.11 s | 281 次/秒 | 2.5 MB | 4.1 MB | 6.6 MB |

口径说明，避免把这张表读成"WAL 快 100 倍"：

1. 这是**本环境的合成负载**，磁盘性能不代表生产。同一仓库里的真机观测（7 GiB 库）
   是另一组数量级，不要混用。
2. 加速来自**两个**因素，且可以分开看：WAL 本身（读不挡写、提交只追加）与
   `synchronous=NORMAL`（提交不再 fsync）。上表第 4 行说明：只开 WAL 而保留 FULL，
   是 7.11 s——仍然比 delete+FULL 快 2.8 倍，但主要收益来自 NORMAL。
   **"少 fsync"就是"断电可能丢最后几笔已提交事务"**，这个交换必须由运维显式接受。
3. `-wal` 常驻 4 MB 左右（autocheckpoint 阈值 1000 页）。小库上它让磁盘占用看起来
   "多了一半"，checkpoint 之后会被截到 0。

---

## 四、本次实施

### 4.1 开关：`DB_WAL`（默认关闭）

`models/init.go`：

- `WALEnv = "DB_WAL"`；取值为 `true` 时 DSN 追加
  `_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)`。
  三条都写进 DSN 而不是开头执行一次——前两条要随每条新连接应用，`busy_timeout`
  本身就是连接级设置。
- `_txlock=immediate` **保留**，并且在 WAL 下更重要：deferred 读事务在快照过期后再写
  会直接返回 `SQLITE_BUSY_SNAPSHOT`，同样绕开 `busy_timeout`。
- `JournalMode` 是 **Init 之后用 `PRAGMA journal_mode` 实测**的值，`WALEnabled()` 读它。
  不以环境变量为准：WAL 是写进库文件头的持久属性（撤掉开关后库仍是 WAL），而开关
  打开了也可能因为在网络文件系统上而没生效。后一种情况会打一条 WARN 并照常启动。

### 4.2 口径修正一：`DBFileSize` 计入 `-wal`

WAL 下主库文件只装**已 checkpoint** 的页，刚提交的部分在 `-wal` 里。只量主库会低估占用。
`service/db_stats.go` 的 `DBFileSize()` 在 `WALEnabled()` 时把 `-wal` 一起算进去
（`-shm` 不算：它只是索引，不是库内容）。

这一处同时是四个调用点的共同口径：状态页的 `file_size`、VACUUM 预检的 2× 估算、
回收前后的大小差、备份校验的基准。

### 4.3 口径修正二：备份判定自动挡住半截备份

`VerifyBackup` 判 `stale` 的规则是"备份比库小"，而库大小现在含 `-wal`。这带来一个
**恰好正确**的副作用：

- 服务运行时直接拷 `.db`（运维习惯里的"拷一个文件就是备份"）会丢掉 `-wal` 里的页，
  拷贝必然小于"主库 + `-wal`"→ 判 `stale`，迁移那道门拒绝放行；
- `PRAGMA wal_checkpoint(TRUNCATE)` 之后 `-wal` 被截到 0，此时同尺寸的拷贝判 `manual`。

两种情形都由 `service/db_wal_test.go` 的 `TestDBFileSizeAndBackupCountWAL` 覆盖。

### 4.4 测试清单

| 用例 | 钉住什么 |
|---|---|
| `models.TestSQLiteDSN` | 开关开/关的 DSN 逐字形状（含"路径已带查询串"） |
| `models.TestInitWALIsPersistentAndReported` | 开关生效；撤掉开关重启后仍是 WAL 且被如实报告 |
| `models.TestWALReadTransactionDoesNotBlockWriter` | delete 挡住 / WAL 不挡，两个方向都断言 |
| `service.TestDBFileSizeAndBackupCountWAL` | `-wal` 计入大小；半截备份判 stale；checkpoint 后可用的备份判 manual |

---

## 五、风险与限制

1. **网络文件系统 / 不共享的挂载**。WAL 依赖 `-shm` 的共享内存语义，NFS/SMB 上要么
   直接失败（我们打 WARN），要么更糟：看起来成功而并发下写坏。**只能跑在本地文件系统上。**
   容器场景要确认 `db/` 是本地卷而不是网络卷。`[码]` `[推]`
2. **断电语义**。`synchronous=NORMAL` 下，断电可能丢掉最后几笔**已提交**的事务
   （库不会坏）。对"请求日志"通常可接受，但它是显式交换，不是免费加速。`[推]`
3. **`-wal` 会涨**。checkpoint 需要"没有读者持有旧快照"。本项目有一类很长的读
   （全表聚合 4.24 秒、状态页轮询），期间 `-wal` 会持续增长；读取结束后才回落。
   当前工作负载下增长量是 MB 级，但慢库/大批量迁移时要按"库的写入量 × 长读时长"估算。`[推]`
4. **备份流程变了**（已收口，但运维要知道）：不能用"拷 `.db`"当日备份，必须先
   checkpoint 或停服务。
5. **VACUUM / `auto_vacuum` 转换在 WAL 下未验证**。`[推]` `DB_VACUUM` 与
   `DB_AUTO_VACUUM_REBUILD=on` 走的是 `PRAGMA auto_vacuum=?; VACUUM`，理论上
   WAL 下可用，但本项目从未在 WAL 库上跑过；开启 `DB_WAL` 的同时要用这两条，
   应当先在副本上验证。
6. **二进制回退是安全的，口径不是**。旧二进制能直接打开 WAL 库（SQLite 文件格式不变），
   但旧代码的 `DBFileSize`/`VerifyBackup` 不含 `-wal`，会**低估占用**并把半截备份
   判成可用。所以回退二进制的同时，要么把库退回 `DELETE`，要么至少不要再跑迁移。
7. **多实例共用同一个库本来就被禁止**（见 `docs/db-busy-lock-fix.md` §7.4），WAL 不改变这一点。

---

## 六、切换与回退步骤

切换（一次性运维动作，建议在低峰）：

```bash
# 1. 备份：停服务后拷文件，或者在线执行 checkpoint 再拷
#    sqlite3 db/llmio.db 'PRAGMA wal_checkpoint(TRUNCATE);'
# 2. 设开关并重启
DB_WAL=true ./llmio
# 3. 确认生效（启动日志 WARN 不出现；或直接问库）
#    期望输出: wal
#    sqlite3 db/llmio.db 'PRAGMA journal_mode;'
```

回退：

```bash
# 停服务后（必须没有其他连接）：
sqlite3 db/llmio.db 'PRAGMA journal_mode=DELETE;'
# 然后去掉 DB_WAL，重启。
# 注意 journal_mode 是库文件头里的属性：只删环境变量不会退回 delete。
```

`[测]` 第二步与最后一步的形状由 `TestInitWALIsPersistentAndReported` 覆盖（撤掉开关仍
报告 `wal`）——这也正是上面那句"只删环境变量不会退回"的依据。

---

## 七、待真机验证清单（本环境做不了）

本环境没有生产库副本（`LLMIO_REAL_DB_COPY` 未设置，`db/llmio.db` 是一个 274 KB 的空库），
以下几项**没有**任何本环境证据。

### 7.1 已自动化的部分（真机上对副本跑）

第七节里能在 `go test` 里做掉的一半已落成用例（`service/db_wal_real_test.go`），对一份
**可写副本**跑——用例会开 WAL、真写、最后把副本翻回 delete：

```bash
# PowerShell
$env:LLMIO_REAL_DB_COPY="D:\llmio-test\work-wal\real.db"
go test ./service/ -run TestWAL_RealCopyAcceptance -v -timeout 30m
```

| 子测试 | 对应清单项 | 回答什么 |
|---|---|---|
| 长读不再挡写 | 第 1/2 条的机制面 | 同一句闯过祸的全表重读持锁期间，写请求零失败、不等满 busy_timeout |
| wal峰值与checkpoint推进 | 第 3、4 条 | 并发写 + 长读 10 秒内 `-wal` 涨到多大；PASSIVE 在长读期间是否 busy、读释放后能否追上；TRUNCATE 截不截得干净 |
| 提交吞吐 | （新增，喂 SaveChatLog 决策） | 真磁盘上 1 KB 行单条自动提交的 µs 级单价——本机 10355 次/秒不代表生产磁盘 |
| 回退到delete | 第 7 条 | `journal_mode=DELETE` 翻回、`-wal` 截干净、写入照常 |

第 5 条（VACUUM 在 WAL 上）也自动化了，但要把整库重写一遍，单独点名才跑：

```bash
$env:LLMIO_REAL_DB_COPY="D:\llmio-test\work-wal\real.db"
$env:LLMIO_WAL_REAL_VACUUM="on"
go test ./service/ -run TestWAL_RealCopyVacuumOnWAL -v -timeout 180m
```

### 7.2 仍需手工的部分

1. **`SQLITE_BUSY` 频次**（唯一直接指标）：开 `DB_WAL` 前后各跑一段真实流量，前后各数一次撞锁痕迹：
   `docker logs <容器> 2>&1 | grep -c "撞锁"`（chat io 撞锁退回明文的重试日志）。预期：开关打开后归零或只剩个位数。
2. **p95 首包延迟**：状态页的延迟分布（`/api/metrics/stats`）在切换前后各取一个同长度时间窗对比。预期改善集中在「状态页轮询 + 迁移/回收并发」时段。
3. **容器文件系统形态**（第 6 条）：`docker exec <容器> df -T /app/db`——必须是本地文件系统（ext4/xfs 等）；nfs/cifs/9p 上 `DB_WAL` 不能开。测试机上 `requireWALMode` 也会挡一道：进不了 WAL 的库会直接红。

复跑本环境已有证据：

```bash
go test ./models/ -run 'WAL' -v
go test ./service/ -run 'TestDBFileSizeAndBackupCountWAL' -v
```

---

## 八、与整体性能规划的关系

在 Phase 2 的定位：**"能不能把并发模型从'排队等锁'换成'读不挡写'"的可行性评估**，
交付物是开关与口径，不是默认行为。它解决的是**稳定性 + 尾延迟**（`SQLITE_BUSY` 一类故障），
对单请求的 CPU/内存几乎没有影响。

如果真机清单全绿，下一步才是"默认开启"的单独决策；在那之前，`DB_WAL` 的默认值保持
`false`，与 `DB_VACUUM`、`DB_AUTO_VACUUM_REBUILD` 一样属于"运维显式选择"。
