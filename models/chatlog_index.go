package models

import "context"

// chatLogsDeletedAtIndex 是 chat_logs.deleted_at 上那条**有害**索引的名字。
//
// 标签已经从 ChatLog.DeletedAt 上摘掉（见 model.go 的字段注释），这个名字只剩
// "清理存量库"这一个用途：老版本建出来的库里它还在，且 AutoMigrate 不会删索引。
const chatLogsDeletedAtIndex = "idx_chat_logs_deleted_at"

// DropChatLogsDeletedAtIndex 摘掉 chat_logs.deleted_at 上的索引。可重复调用。
//
// 这条索引不是"可有可无"，是**有害**的。chat_logs 的每一条删除路径都是
// Unscoped 硬删除（service/log_cleanup.go 的定时清理、handler/api.go 的按 id
// 清理），deleted_at 因此恒为 NULL。于是这条索引：
//
//   - 对任何查询都命中整张表，窄化不了任何东西；
//   - 却因为"看起来选择性很高"稳居查询计划器首选——GORM 会给每条 chat_logs
//     读语句自动补 `deleted_at IS NULL`，分析页那条按时间窗取数的查询因此放着
//     现成的 idx_chat_logs_created_at 不用，改成"扫完 deleted_at 索引 + 临时
//     B 树排序"，两条筛选下拉的 DISTINCT 也一起退化成全表扫。
//
// 实测（20 万行、无 sqlite_stat1，即线上真实状态；临时库上跑 EXPLAIN QUERY PLAN）：
//
//	按时间窗取数           114ms → 16ms（改走 idx_chat_logs_created_at，临时 B 树消失）
//	DISTINCT provider_model  100ms →  50ms（改走同名覆盖索引）
//	DISTINCT user_agent       58ms →  47ms（同上）
//
// 线上 1.34 GB 的库上第一条是 2.07 秒。回滚日志模式下读事务挡写，这 2 秒就是
// 写请求的排队时间：INSERT 等满 busy_timeout 报 SQLITE_BUSY、请求积压，最终把
// 128 MiB 的容器顶到 OOMKilled（2026-10-29 的事故：容器重启 2 次，cgroup 峰值
// 134,172,672 B，占上限 99.97%）。
// 少一条索引顺带省掉每一次 chat_logs 写入的索引维护成本——这一列既然恒为 NULL，
// 写进去的每一个索引项都是白写。
//
// 时序前提与 EnsureChatIOLogIDIndex 相同：调用点在 Init 里、监听端口之前，
// 此刻没有在途请求。DROP INDEX 只改 B 树指针，对大表同样是瞬时操作。
func DropChatLogsDeletedAtIndex(ctx context.Context) error {
	return DB.WithContext(ctx).Exec("DROP INDEX IF EXISTS " + chatLogsDeletedAtIndex).Error
}

// chatLogsPrefixRedundantColumns 是 chat_logs 上与复合索引最左前缀重复的列。
//
// 老版本的标签写成 `gorm:"index;index:idx_chat_logs_xxx_created,priority:1"`，
// 同一列上会同时建出单列与复合两条索引。单列等值查询（如 WHERE name = ?）走
// 复合索引的前缀与走单列索引完全等价，单列那条从此只是双倍的写入期维护成本。
// 标签已从 ChatLog 上摘掉（见 model.go），AutoMigrate 不会再建新的；存量库上
// GORM 按默认命名建出来的 idx_chat_logs_name / idx_chat_logs_status /
// idx_chat_logs_auth_key_id 由下面的函数在启动时摘掉。
var chatLogsPrefixRedundantColumns = map[string]bool{
	"name":        true,
	"status":      true,
	"auth_key_id": true,
}

// DropChatLogsPrefixRedundantIndexes 摘掉 chat_logs 上前缀冗余的单列索引。可重复调用。
//
// 按**列构成**检测而不是按名字：要删的就是"只含 name / status / auth_key_id
// 一列的普通二级索引"，idx_chat_logs_name 这些默认名只是 GORM 的命名产物，
// 以库里实际检测到的为准。这样即使某个库上的同名索引被重建过（列构成没变），
// 也照样能被识别；反之，列构成对不上的同名索引绝不动。
//
// 只删同时满足这三条的索引：
//   - origin = 'c'（CREATE INDEX 建的；'u'/'pk' 是约束自带的，碰都不碰）；
//   - unique = 0（唯一索引牵扯约束语义，不是性能问题，绝不动）；
//   - index_info 恰好一列、且列在冗余清单里（三个复合索引各两列，天然不匹配）。
//
// 时序前提与 DropChatLogsDeletedAtIndex 相同：调用点在 Init 里、监听端口之前，
// 此刻没有在途请求。DROP INDEX 只改 B 树指针，对大表是瞬时操作。
func DropChatLogsPrefixRedundantIndexes(ctx context.Context) error {
	var indexes []struct {
		Name   string
		Unique int
		Origin string
	}
	if err := DB.WithContext(ctx).Raw("PRAGMA index_list('chat_logs')").Scan(&indexes).Error; err != nil {
		return err
	}

	var drop []string
	for _, idx := range indexes {
		if idx.Origin != "c" || idx.Unique != 0 {
			continue
		}
		var cols []struct{ Name string }
		if err := DB.WithContext(ctx).Raw("PRAGMA index_info(\"" + idx.Name + "\")").Scan(&cols).Error; err != nil {
			return err
		}
		if len(cols) == 1 && chatLogsPrefixRedundantColumns[cols[0].Name] {
			drop = append(drop, idx.Name)
		}
	}
	for _, name := range drop {
		if err := DB.WithContext(ctx).Exec("DROP INDEX IF EXISTS \"" + name + "\"").Error; err != nil {
			return err
		}
	}
	return nil
}
