package models

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// initModelAt 在指定路径上走一次真实初始化（= 真机启动），
// 并在测试结束时关连接、还原包级状态。
func initModelAt(t *testing.T, path string) {
	t.Helper()
	prevDB, prevPath := DB, DBPath
	closeCurrentDB(t)
	Init(context.Background(), path)
	t.Cleanup(func() {
		if DB != nil {
			if sqlDB, err := DB.DB(); err == nil {
				_ = sqlDB.Close()
			}
		}
		DB, DBPath = prevDB, prevPath
	})
}

// chat_logs 的每一条删除路径都是 Unscoped 硬删除，deleted_at 恒为 NULL。
// 这条索引因此命中整张表，却会被查询计划器优先选中（详见 DropChatLogsDeletedAtIndex）。
func TestFreshDBCreatesNoChatLogsDeletedAtIndex(t *testing.T) {
	initModelAt(t, filepath.Join(t.TempDir(), "fresh.db"))

	if indexExists(t, DB, "idx_chat_logs_deleted_at") {
		t.Fatal("新库上又建出了 idx_chat_logs_deleted_at：它会把分析页取数逼成全表扫")
	}
	// 该留的索引不能被顺手弄丢：时间窗取数靠它
	if !indexExists(t, DB, "idx_chat_logs_created_at") {
		t.Fatal("idx_chat_logs_created_at 不见了：分析页取数会退化成全表扫")
	}
}

// 存量库（老版本建的）上的旧索引要在启动迁移里被摘掉，且摘掉时不动数据。
func TestInitDropsLegacyChatLogsDeletedAtIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-deleted-at-idx.db")
	initModelAt(t, path)

	// 模拟老版本：标签还在时 GORM 建出来的就是这条
	if err := DB.Exec("CREATE INDEX idx_chat_logs_deleted_at ON chat_logs(deleted_at)").Error; err != nil {
		t.Fatalf("造旧索引失败：%v", err)
	}
	if err := DB.Exec(`INSERT INTO chat_logs (created_at, name, status) VALUES ("2026-10-29 00:00:00", "m", "success")`).Error; err != nil {
		t.Fatalf("造种子行失败：%v", err)
	}

	// 真机上就是"升级镜像后重启"这一步
	closeCurrentDB(t)
	Init(context.Background(), path)

	if indexExists(t, DB, "idx_chat_logs_deleted_at") {
		t.Fatal("升级之后旧索引还在：分析页取数会继续全表扫")
	}
	var n int64
	if err := DB.Raw(`SELECT COUNT(*) FROM chat_logs`).Scan(&n).Error; err != nil {
		t.Fatalf("查行数失败：%v", err)
	}
	if n != 1 {
		t.Fatalf("摘索引动了数据：行数 %d，want 1", n)
	}
}

// 迁移函数必须可重复调用：每次启动都会跑。
func TestDropChatLogsDeletedAtIndexIsIdempotent(t *testing.T) {
	initModelAt(t, filepath.Join(t.TempDir(), "idempotent.db"))
	ctx := context.Background()

	for i := range 2 {
		if err := DropChatLogsDeletedAtIndex(ctx); err != nil {
			t.Fatalf("第 %d 次调用失败：%v", i+1, err)
		}
	}
	if indexExists(t, DB, "idx_chat_logs_deleted_at") {
		t.Fatal("调用之后索引仍然存在")
	}
}

// 存量库上 GORM 默认命名的那三条单列索引（idx_chat_logs_name / status /
// auth_key_id）要在启动迁移里被摘掉：它们是三个复合索引的最左前缀，单列等值
// 查询复合索引完全覆盖，留着只是双倍的写入期维护成本。摘的同时复合索引必须
// 原样保留，数据也不能动。
func TestInitDropsPrefixRedundantChatLogsIndexes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-prefix-idx.db")
	initModelAt(t, path)

	// 新代码的标签已不带单列索引，这里手工把存量库的形态补回来：
	// 老版本 `gorm:"index;index:...,priority:1"` 标签建出来的就是这三条。
	legacy := []struct{ name, col string }{
		{"idx_chat_logs_name", "name"},
		{"idx_chat_logs_status", "status"},
		{"idx_chat_logs_auth_key_id", "auth_key_id"},
	}
	for _, idx := range legacy {
		if err := DB.Exec("CREATE INDEX " + idx.name + " ON chat_logs(" + idx.col + ")").Error; err != nil {
			t.Fatalf("造旧索引 %s 失败：%v", idx.name, err)
		}
	}
	if err := DB.Exec(`INSERT INTO chat_logs (created_at, name, status, auth_key_id)
		VALUES ("2026-10-29 00:00:00", "m", "success", 7)`).Error; err != nil {
		t.Fatalf("造种子行失败：%v", err)
	}

	// 真机上就是"升级镜像后重启"这一步
	closeCurrentDB(t)
	Init(context.Background(), path)

	for _, idx := range legacy {
		if indexExists(t, DB, idx.name) {
			t.Fatalf("升级之后冗余单列索引 %s 还在：每次写入都要多付一份维护成本", idx.name)
		}
	}
	// 复合索引不能被顺手动到：列序错了对前缀等值查询就不再是等价覆盖。
	// created_at 的单列索引也必须留住——它在三个复合索引里都是第二列。
	want := map[string][]string{
		"idx_chat_logs_created_at":     {"created_at"},
		"idx_chat_logs_status_created": {"status", "created_at"},
		"idx_chat_logs_key_created":    {"auth_key_id", "created_at"},
		"idx_chat_logs_name_created":   {"name", "created_at"},
	}
	for index, wantCols := range want {
		if got := indexColumns(t, DB, index); strings.Join(got, ",") != strings.Join(wantCols, ",") {
			t.Fatalf("索引 %s 的列应为 %v，实得 %v", index, wantCols, got)
		}
	}
	var n int64
	if err := DB.Raw(`SELECT COUNT(*) FROM chat_logs`).Scan(&n).Error; err != nil {
		t.Fatalf("查行数失败：%v", err)
	}
	if n != 1 {
		t.Fatalf("摘索引动了数据：行数 %d，want 1", n)
	}
}

// 热路径查询必须走对应复合索引，且不产生临时 B 树排序。
// 退化不会报错、只会变慢，所以用 EXPLAIN QUERY PLAN 钉住
// （参照 service/stats_query_plan_test.go 的写法）。
func TestChatLogHotPathQueryPlansUseCompositeIndexes(t *testing.T) {
	initModelAt(t, filepath.Join(t.TempDir(), "plan.db"))

	cases := []struct {
		name      string
		sql       string
		wantIndex string
	}{
		{"按模型下钻", `SELECT * FROM chat_logs WHERE name = 'gpt-4o' AND created_at >= '2026-01-01' ORDER BY created_at DESC`, "idx_chat_logs_name_created"},
		{"趋势与成功率", `SELECT * FROM chat_logs WHERE status = 'success' AND created_at >= '2026-01-01' ORDER BY created_at DESC`, "idx_chat_logs_status_created"},
		{"按 Key 下钻", `SELECT * FROM chat_logs WHERE auth_key_id = 1 AND created_at >= '2026-01-01' ORDER BY created_at DESC`, "idx_chat_logs_key_created"},
		// 单列等值也要命中复合索引的前缀：这正是摘掉单列索引的依据——
		// 如果计划器不认前缀，摘索引就不成立。
		{"纯单列等值", `SELECT * FROM chat_logs WHERE name = 'gpt-4o'`, "idx_chat_logs_name_created"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var plan []struct{ Detail string }
			if err := DB.Raw("EXPLAIN QUERY PLAN " + tc.sql).Scan(&plan).Error; err != nil {
				t.Fatalf("EXPLAIN QUERY PLAN 失败：%v\nSQL: %s", err, tc.sql)
			}
			details := make([]string, 0, len(plan))
			for _, p := range plan {
				details = append(details, p.Detail)
			}
			joined := strings.Join(details, " | ")
			if !strings.Contains(joined, tc.wantIndex) {
				t.Fatalf("没走 %s：%s", tc.wantIndex, joined)
			}
			if strings.Contains(joined, "TEMP B-TREE") {
				t.Fatalf("出现了临时 B 树排序（说明索引只用来过滤、没提供顺序）：%s", joined)
			}
		})
	}
}
