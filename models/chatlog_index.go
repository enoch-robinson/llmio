package models

import "context"

// chatLogsDeletedAtIndex 是 chat_logs.deleted_at 上那条**有害**索引的名字。
//
// 标签已经从 ChatLog.DeletedAt 上摘掉（见 model.go 的字段注释），这个名字只剩
// "清理存量库"这一个用途：老版本建出来的库里它还在，且 AutoMigrate 不会删索引。
const chatLogsDeletedAtIndex = "idx_chat_logs_deleted_at"

// DropChatLogsDeletedAtIndex 摘掉 chat_logs.deleted_at 上的索引。可重复调用。
//
// chat_logs 每条删除路径都是 Unscoped 硬删除，deleted_at 恒为 NULL：这条索引
// 对任何查询都窄化不了，却因"看起来选择性高"被计划器首选，把时间窗查询拖成
// 全索引扫描 + 临时 B 树排序。实测 20 万行：按时间窗取数 114ms → 16ms；线上
// 1.34GB 库上 2.07s，读事务挡写最终把 128MiB 容器顶到 OOMKilled（2026-10-29
// 事故）。
//
// 时序前提：调用点在 Init 里、监听端口之前，无在途请求；DROP INDEX 只改 B 树
// 指针，瞬时完成。
func DropChatLogsDeletedAtIndex(ctx context.Context) error {
	return DB.WithContext(ctx).Exec("DROP INDEX IF EXISTS " + chatLogsDeletedAtIndex).Error
}

// chatLogsPrefixRedundantColumns 与复合索引最左前缀重复的列：老版本标签把它们
// 同时建成单列 + 复合两条索引，单列等值查询走复合前缀完全等价，单列只是双倍
// 写入期维护成本。标签已从 ChatLog 摘掉，存量库由下面的函数在启动时摘掉。
var chatLogsPrefixRedundantColumns = map[string]bool{
	"name":        true,
	"status":      true,
	"auth_key_id": true,
}

// DropChatLogsPrefixRedundantIndexes 摘掉 chat_logs 上前缀冗余的单列索引。可重复调用。
//
// 按**列构成**检测而非名字：同名但列构成对不上的索引绝不动。只删同时满足
// 三条的索引：origin='c'（CREATE INDEX 建的，约束自带的碰都不碰）、unique=0、
// 恰好一列且在冗余清单里。时序前提同 DropChatLogsDeletedAtIndex。
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
