package models

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// closeOnCleanup 注册在测试结束时关闭连接：Windows 上文件被占用时 t.TempDir
// 的清理会失败；Cleanup LIFO 保证这里注册的关闭先于 TempDir 清理执行。
func closeOnCleanup(t *testing.T, db *gorm.DB) {
	t.Helper()
	t.Cleanup(func() {
		if db == nil {
			return
		}
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
}

func TestInit_BackfillsAuthKeyIOLogToFalse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "llmio.db")

	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open seed database: %v", err)
	}
	closeOnCleanup(t, db)

	if err := db.Exec(`
		CREATE TABLE auth_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at DATETIME,
			updated_at DATETIME,
			deleted_at DATETIME,
			name TEXT,
			key TEXT,
			status NUMERIC,
			allow_all NUMERIC,
			models TEXT,
			expires_at DATETIME,
			usage_count INTEGER,
			last_used_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("failed to create legacy auth_keys table: %v", err)
	}

	if err := db.Exec(`INSERT INTO auth_keys (name, key, status, allow_all, usage_count) VALUES (?, ?, ?, ?, ?)`,
		"legacy-project", "legacy-key", true, true, 0,
	).Error; err != nil {
		t.Fatalf("failed to seed legacy auth key: %v", err)
	}

	Init(context.Background(), path)
	// Init 另开的连接同样需要关闭
	closeOnCleanup(t, DB)

	authKey, err := gorm.G[AuthKey](DB).Where("key = ?", "legacy-key").First(context.Background())
	if err != nil {
		t.Fatalf("failed to load migrated auth key: %v", err)
	}

	if authKey.IOLog == nil {
		t.Fatal("expected io_log to be backfilled")
	}
	if *authKey.IOLog {
		t.Fatal("expected io_log default to false")
	}
}

// chat_logs.backup 不归一的话老库存量行 NULL 扫描 bool 直接报错；
// 去掉 init.go 里的回填这条测试就会红。
func TestInit_BackfillsChatLogBackupToFalse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "llmio.db")

	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open seed database: %v", err)
	}
	closeOnCleanup(t, db)

	// 老 chat_logs 表：没有 backup 列
	if err := db.Exec(`
		CREATE TABLE chat_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at DATETIME,
			updated_at DATETIME,
			deleted_at DATETIME,
			name TEXT,
			trace_id TEXT,
			provider_model TEXT,
			provider_name TEXT,
			status TEXT,
			style TEXT,
			chat_io NUMERIC,
			auth_key_id INTEGER
		)
	`).Error; err != nil {
		t.Fatalf("failed to create legacy chat_logs table: %v", err)
	}

	if err := db.Exec(`INSERT INTO chat_logs (name, provider_name, status, auth_key_id) VALUES (?, ?, ?, ?)`,
		"legacy-model", "legacy-provider", "success", 0,
	).Error; err != nil {
		t.Fatalf("failed to seed legacy chat log: %v", err)
	}

	Init(context.Background(), path)
	closeOnCleanup(t, DB)

	chatLog, err := gorm.G[ChatLog](DB).Where("name = ?", "legacy-model").First(context.Background())
	if err != nil {
		t.Fatalf("failed to load migrated chat log（backup 为 NULL 时这里会因 bool 扫描失败而报错）: %v", err)
	}
	if chatLog.Backup {
		t.Fatal("expected backup default to false")
	}
}

// chat_logs 兼容回填只该在升级时跑一次：NULL 行被回填、标记落库、标记在则二次启动跳过。
func TestInitChatLogsBackfillRunsOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backfill-gate.db")

	// 老库形态：有 auth_key_id 列但没有 backup 列
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open seed database: %v", err)
	}
	closeOnCleanup(t, db)

	if err := db.Exec(`
		CREATE TABLE chat_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at DATETIME,
			updated_at DATETIME,
			deleted_at DATETIME,
			name TEXT,
			trace_id TEXT,
			provider_model TEXT,
			provider_name TEXT,
			status TEXT,
			style TEXT,
			chat_io NUMERIC,
			auth_key_id INTEGER
		)
	`).Error; err != nil {
		t.Fatalf("failed to create legacy chat_logs table: %v", err)
	}
	if err := db.Exec(`INSERT INTO chat_logs (name, provider_name, status, auth_key_id) VALUES
		('log-null', 'prov', 'success', NULL),
		('log-user', 'prov', 'error', 7)`).Error; err != nil {
		t.Fatalf("failed to seed legacy chat logs: %v", err)
	}
	if sqlDB, err := db.DB(); err == nil {
		_ = sqlDB.Close()
	}

	Init(context.Background(), path)
	closeOnCleanup(t, DB)

	t.Run("NULL 行被回填且用户填过的值不动", func(t *testing.T) {
		if got := chatLogsNullRows(t, "auth_key_id"); got != 0 {
			t.Fatalf("auth_key_id 的 NULL 应被回填干净，实剩 %d 行", got)
		}
		if got := chatLogsNullRows(t, "backup"); got != 0 {
			t.Fatalf("backup 的 NULL 应被回填干净，实剩 %d 行", got)
		}
		var n int64
		if err := DB.Raw(`SELECT COUNT(*) FROM chat_logs WHERE name = 'log-user' AND auth_key_id = 7`).Scan(&n).Error; err != nil {
			t.Fatalf("count: %v", err)
		}
		if n != 1 {
			t.Fatal("用户填过的 auth_key_id 被回填覆盖了")
		}
	})

	t.Run("完成标记落库", func(t *testing.T) {
		if got := countRowsWhere(t, "configs", "key = ?", KeyChatLogsBackfillState); got != 1 {
			t.Fatalf("回填完成标记应有且只有 1 行，实得 %d 行", got)
		}
	})

	// 故意改回一行 NULL：二次启动后应原样留着，即回填被跳过的直接证据
	if err := DB.Exec(`UPDATE chat_logs SET auth_key_id = NULL WHERE name = 'log-null'`).Error; err != nil {
		t.Fatalf("造二次启动前的 NULL 行失败：%v", err)
	}
	closeCurrentDB(t)
	Init(context.Background(), path)

	t.Run("二次启动跳过回填", func(t *testing.T) {
		if got := chatLogsNullRows(t, "auth_key_id"); got != 1 {
			t.Fatalf("标记已存在，回填不该再跑：实剩 %d 行 NULL，want 1", got)
		}
		if got := countRowsWhere(t, "configs", "key = ?", KeyChatLogsBackfillState); got != 1 {
			t.Fatalf("标记应仍只有 1 行，实得 %d 行", got)
		}
	})
}

// chatLogsNullRows 用原生 SQL：NULL 的 uint/bool 扫进结构体会直接报错。
func chatLogsNullRows(t *testing.T, col string) int64 {
	t.Helper()
	var n int64
	if err := DB.Raw("SELECT COUNT(*) FROM chat_logs WHERE " + col + " IS NULL").Scan(&n).Error; err != nil {
		t.Fatalf("数 %s 的 NULL 失败：%v", col, err)
	}
	return n
}

// model_with_providers.backup 同理：NULL 的备用标记在调度分池里读不出来。
func TestInit_BackfillsModelWithProviderBackupToFalse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "llmio.db")

	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open seed database: %v", err)
	}
	closeOnCleanup(t, db)

	if err := db.Exec(`
		CREATE TABLE model_with_providers (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at DATETIME,
			updated_at DATETIME,
			deleted_at DATETIME,
			model_id INTEGER,
			provider_id INTEGER,
			provider_model TEXT,
			weight INTEGER
		)
	`).Error; err != nil {
		t.Fatalf("failed to create legacy model_with_providers table: %v", err)
	}

	if err := db.Exec(`INSERT INTO model_with_providers (model_id, provider_id, provider_model, weight) VALUES (?, ?, ?, ?)`,
		1, 1, "legacy-model", 1,
	).Error; err != nil {
		t.Fatalf("failed to seed legacy association: %v", err)
	}

	Init(context.Background(), path)
	closeOnCleanup(t, DB)

	mp, err := gorm.G[ModelWithProvider](DB).Where("provider_model = ?", "legacy-model").First(context.Background())
	if err != nil {
		t.Fatalf("failed to load migrated association（backup 为 NULL 时这里会因 bool 扫描失败而报错）: %v", err)
	}
	if mp.Backup == nil {
		t.Fatal("expected backup to be backfilled")
	}
	if *mp.Backup {
		t.Fatal("expected backup default to false")
	}
}
