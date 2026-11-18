# Implementation Plan: 剩余性能改进第二批（统计瘦身 + 热路径小项）

来源：2026-11 性能审计报告的剩余项；上一批 6 个提交已在 `perf/memory-and-hotpath` 分支（bd33fc3..18b2bd6）。

## Overview

五个独立任务：统计响应瘦身（删两份无消费的全量样本列表 + 首包样本改服务端直方图）、分组 P95 改有界 reservoir 采样、count_tokens 配置缓存与上游超时、峰谷计费时钟预解析、转发错误路径的字节闸门。每个任务独立可验证，完成后留测试/基准作为正确性与收益依据。

## Architecture Decisions

1. **`latency.tps.list` / `latency.proxyMs.list` 直接删除**（前端 0 消费，已 grep 核实）；`latency.firstChunk.list` 替换为 `histogram{edges,counts,total}`。直方图分桶沿用前端 `FirstChunkHistogram` 现有边界（`0,0.5,1,2,3,5,8,10,15,20,30,60` 秒，左闭右开、末桶兜底），边界常量放 Go 侧并注释与前端对齐；前端组件改为接收预分桶数据，标签生成、尾部空桶裁剪、顺序色阶留在前端。
2. **分组 P95 用 reservoir sampling（K=1024，按组键 FNV 播种）**：组样本 ≤ K 时与旧实现逐位一致（精确），超大组内存从 O(样本数) 降到 O(K)≈8KB/组；固定种子保证确定性。比固定分桶直方图实现更简单且小组零精度损失。
3. **count_tokens**：解析后的配置以 30s TTL 缓存（沿用 peak 范式），经 `UpdateConfigByKey` 写后失效；`http.DefaultClient` 换成带 30s Timeout 的包级 client。配置行不存在与坏 JSON 均不进缓存（前者使创建后立即可见，后者修好后立即可读），两者错误提示用 `parseConfigError` 区分。
4. **峰谷计费**（实施中依证据修订）：原计划假设 resolver 会被复用，但热路径上 terms 每请求从关联行新鲜加载、resolver 每请求重建，构造时预解析收益存疑。profile 证实单次判定 ~78% CPU 在 `time.LoadLocation` 的 tzdata 文件 syscall（容器无磁盘 tzdata 回退内嵌解析），因此落地为三件：构造时预解析时钟（复用场景 PreviewSchedule 逐分钟采样直接受益）+ `tzLocation` 进程级时区缓存（时区名为配置枚举值，集合有界，IANA 数据进程内不变）+ `IsWorkday` 空 overrides 快路径跳过 Format 分配；`effectiveTime` 与 `models.EffectiveTime` 语义一致（空/坏名回原值）。
5. **错误路径**：非 200 错误体读取包 `LimitReader(1MB)`（与 quota 侧 `maxHTTPBodyBytes` 同量级）；ErrorMatcher 匹配只采样前 64KB，转发体用 `MultiReader` 无损重拼（body ≤64KB 时行为与现在逐字节一致，更大时 matcher 只见前缀——注释写明权衡）。实施中发现旧实现未命中分支用 `NopCloser` 丢弃了原 body 的 Close，读过的连接既不复用也不释放；新 `readCloser` 组合保留 Close 链，一并修复。

## Task List

### Phase 1: 统计瘦身
- [x] Task 1: 删 tps/proxy 的 list，firstChunk 改服务端直方图（Go + webui 契约同步）
- [x] Task 2: groupAcc P95 改 K=1024 reservoir 采样
- [x] Checkpoint: 统计相关测试全绿 + webui build 通过

### Phase 2: 热路径小项
- [x] Task 3: count_tokens 配置缓存 + 上游超时
- [x] Task 4: 峰谷计费时钟预解析（基准先测基线再对比）
- [x] Task 5: 转发错误路径字节闸门（1MB 错误体 + 64KB matcher 采样）
- [x] Checkpoint: 全部测试 + race + 全仓构建通过

## Risks and Mitigations

| 风险 | 影响 | 缓解 |
|------|------|------|
| stats 契约变更影响未知第三方消费者 | 中 | webui 是唯一已知消费方（grep 核实）；直方图语义与旧直方图逐桶一致 |
| 直方图边界前后端漂移 | 低 | 边界只在 Go 定义、随响应下发 edges，前端不再持有常量 |
| reservoir 随机性导致 P95 不可复现 | 中 | FNV(组键) 固定播种；小组（≤K）精确等于旧值 |
| ErrorMatcher 只见前 64KB 漏配超长尾部标记 | 低 | 常见错误标记都在响应前部；注释与测试写明边界 |
| pnpm build 环境不可用 | 低 | node_modules 已存在；失败则报告并退回 go 侧验证 + 手动说明 |
| tzLocation 进程级缓存不感知 IANA 时区规则更新 | 极低 | 进程生命周期内时区规则变更概率≈0；需感知时重启即可 |
| count_tokens/峰谷 TTL 缓存使绕过 API 的直接改库延迟可见 | 低 | 与既有 peak/authkey 缓存同一范式与 TTL 量级；管理台写路径已挂失效钩子 |

## Open Questions

- 无（统计契约变更已在上一轮审计与用户确认范围内）。
