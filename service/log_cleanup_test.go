package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/atopos31/llmio/models"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupLogCleanupTestDB(t *testing.T) func() {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}

	if err := db.AutoMigrate(&models.ChatLog{}, &models.ChatIO{}, &models.Config{}, &models.LogCleanupRecord{}); err != nil {
		t.Fatalf("failed to migrate test database: %v", err)
	}

	models.DB = db
	return func() {
		models.DB = nil
	}
}

func TestCleanLogsByDays(t *testing.T) {
	cleanup := setupLogCleanupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	oldLog := models.ChatLog{Name: "old-log", Status: "success"}
	if err := gorm.G[models.ChatLog](models.DB).Create(ctx, &oldLog); err != nil {
		t.Fatalf("failed to create old log: %v", err)
	}
	if err := models.DB.Model(&models.ChatLog{}).Where("id = ?", oldLog.ID).Update("created_at", time.Now().AddDate(0, 0, -40)).Error; err != nil {
		t.Fatalf("failed to age old log: %v", err)
	}

	newLog := models.ChatLog{Name: "new-log", Status: "success"}
	if err := gorm.G[models.ChatLog](models.DB).Create(ctx, &newLog); err != nil {
		t.Fatalf("failed to create new log: %v", err)
	}
	if err := models.DB.Model(&models.ChatLog{}).Where("id = ?", newLog.ID).Update("created_at", time.Now().AddDate(0, 0, -2)).Error; err != nil {
		t.Fatalf("failed to date new log: %v", err)
	}

	if err := gorm.G[models.ChatIO](models.DB).Create(ctx, &models.ChatIO{LogId: oldLog.ID, Input: models.BodyBytes("old")}); err != nil {
		t.Fatalf("failed to create old chat io: %v", err)
	}
	if err := gorm.G[models.ChatIO](models.DB).Create(ctx, &models.ChatIO{LogId: newLog.ID, Input: models.BodyBytes("new")}); err != nil {
		t.Fatalf("failed to create new chat io: %v", err)
	}

	deletedCount, err := CleanLogsByDays(ctx, 30)
	if err != nil {
		t.Fatalf("CleanLogsByDays failed: %v", err)
	}
	if deletedCount != 1 {
		t.Fatalf("expected 1 deleted log, got %d", deletedCount)
	}

	var oldLogCount int64
	if err := models.DB.Unscoped().Model(&models.ChatLog{}).Where("id = ?", oldLog.ID).Count(&oldLogCount).Error; err != nil {
		t.Fatalf("failed to count old log: %v", err)
	}
	if oldLogCount != 0 {
		t.Fatalf("expected old log to be deleted, got count %d", oldLogCount)
	}

	var newLogCount int64
	if err := models.DB.Unscoped().Model(&models.ChatLog{}).Where("id = ?", newLog.ID).Count(&newLogCount).Error; err != nil {
		t.Fatalf("failed to count new log: %v", err)
	}
	if newLogCount != 1 {
		t.Fatalf("expected new log to remain, got count %d", newLogCount)
	}

	var oldChatIOCount int64
	if err := models.DB.Unscoped().Model(&models.ChatIO{}).Where("log_id = ?", oldLog.ID).Count(&oldChatIOCount).Error; err != nil {
		t.Fatalf("failed to count old chat io: %v", err)
	}
	if oldChatIOCount != 0 {
		t.Fatalf("expected old chat io to be deleted, got count %d", oldChatIOCount)
	}

	var newChatIOCount int64
	if err := models.DB.Unscoped().Model(&models.ChatIO{}).Where("log_id = ?", newLog.ID).Count(&newChatIOCount).Error; err != nil {
		t.Fatalf("failed to count new chat io: %v", err)
	}
	if newChatIOCount != 1 {
		t.Fatalf("expected new chat io to remain, got count %d", newChatIOCount)
	}
}

func TestGetLogCleanupPolicyDefaultsWhenMissing(t *testing.T) {
	cleanup := setupLogCleanupTestDB(t)
	defer cleanup()

	policy, err := GetLogCleanupPolicy(context.Background())
	if err != nil {
		t.Fatalf("GetLogCleanupPolicy failed: %v", err)
	}
	if policy == nil {
		t.Fatal("expected policy pointer")
	}
	if policy.Enabled {
		t.Fatal("expected default policy to be disabled")
	}
	if policy.RetentionDays != defaultLogRetentionDays {
		t.Fatalf("expected default retention days %d, got %d", defaultLogRetentionDays, policy.RetentionDays)
	}
}

func TestGetLogCleanupPolicyReadsConfig(t *testing.T) {
	cleanup := setupLogCleanupTestDB(t)
	defer cleanup()

	value, err := json.Marshal(models.LogCleanupPolicy{
		Enabled:       true,
		RetentionDays: 7,
	})
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	if err := gorm.G[models.Config](models.DB).Create(context.Background(), &models.Config{
		Key:   models.KeyLogCleanupPolicy,
		Value: string(value),
	}); err != nil {
		t.Fatalf("failed to create config: %v", err)
	}

	policy, err := GetLogCleanupPolicy(context.Background())
	if err != nil {
		t.Fatalf("GetLogCleanupPolicy failed: %v", err)
	}
	if policy == nil {
		t.Fatal("expected policy pointer")
	}
	if !policy.Enabled {
		t.Fatal("expected policy to be enabled")
	}
	if policy.RetentionDays != 7 {
		t.Fatalf("expected retention days 7, got %d", policy.RetentionDays)
	}
}

// seedChatLogs 批量造数据：expired 条过期（created_at 早于按 30 天保留期推出的
// cutoff）、fresh 条新鲜，每条都配一条 ChatIO，方便断言"日志和 IO 同生共死"。
// 返回 cutoff（CleanLogsByDays(ctx, 30) 将采用的过期分界）。
func seedChatLogs(t *testing.T, expired, fresh int) time.Time {
	t.Helper()
	cutoff := time.Now().AddDate(0, 0, -30)

	logs := make([]models.ChatLog, 0, expired+fresh)
	for i := 0; i < expired; i++ {
		logs = append(logs, models.ChatLog{Name: "expired", CreatedAt: cutoff.AddDate(0, 0, -1)})
	}
	for i := 0; i < fresh; i++ {
		logs = append(logs, models.ChatLog{Name: "fresh", CreatedAt: time.Now()})
	}
	if err := models.DB.Create(&logs).Error; err != nil {
		t.Fatalf("failed to seed chat logs: %v", err)
	}

	ios := make([]models.ChatIO, 0, len(logs))
	for _, l := range logs {
		ios = append(ios, models.ChatIO{LogId: l.ID, Input: models.BodyBytes("body")})
	}
	if err := models.DB.Create(&ios).Error; err != nil {
		t.Fatalf("failed to seed chat ios: %v", err)
	}
	return cutoff
}

func countRows(t *testing.T, model any, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := models.DB.Unscoped().Model(model).Where(query, args...).Count(&n).Error; err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	return n
}

// countOrphanChatIOs 数孤儿 IO：log_id 指向的 chat_log 已经不在了的行。
// 每批事务保证了"删日志必先删 IO"，孤儿数恒应为 0。
func countOrphanChatIOs(t *testing.T) int64 {
	t.Helper()
	var n int64
	if err := models.DB.Raw(
		`SELECT count(*) FROM chat_ios WHERE log_id NOT IN (SELECT id FROM chat_logs)`,
	).Scan(&n).Error; err != nil {
		t.Fatalf("failed to count orphan chat ios: %v", err)
	}
	return n
}

// TestCleanLogsByDaysBatched 覆盖分批删除的三条主路径。
//
// 批量起点用包内 var logCleanupBatchStart 调小来强制多批；取消点挂在 gorm 的
// Query 回调上——CleanLogsByDays 里唯一走 Query 链的语句就是选批的 Pluck，
// 第 N 次 Pluck 时取消，等价于"第 N-1 批已提交、下一批还没开事务"，完全
// 确定性，不靠 sleep 时序。
func TestCleanLogsByDaysBatched(t *testing.T) {
	cases := []struct {
		name          string
		expired       int
		fresh         int
		batchStart    int // ≤0 表示不动全局起点
		cancelOnPluck int // 第几次选批时取消 ctx（0 表示不取消）
		wantDeleted   int64
	}{
		// 150 > 起始批量 64，必然跨多批；就算自适应涨上去，第一批也只拿 64 条。
		{name: "默认起点跨多批删完过期行", expired: 150, fresh: 3, wantDeleted: 150},
		{name: "强制小批量总数正确", expired: 40, fresh: 5, batchStart: 3, wantDeleted: 40},
		{name: "ctx取消返回部分计数且无半删", expired: 5, fresh: 2, batchStart: 1, cancelOnPluck: 2, wantDeleted: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanup := setupLogCleanupTestDB(t)
			defer cleanup()

			if tc.batchStart > 0 {
				prev := logCleanupBatchStart
				logCleanupBatchStart = tc.batchStart
				defer func() { logCleanupBatchStart = prev }()
			}

			cutoff := seedChatLogs(t, tc.expired, tc.fresh)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelOnPluck > 0 {
				fires := 0
				err := models.DB.Callback().Query().After("gorm:query").Register("test_cancel_on_pluck", func(tx *gorm.DB) {
					fires++
					if fires == tc.cancelOnPluck {
						cancel()
					}
				})
				if err != nil {
					t.Fatalf("failed to register cancel hook: %v", err)
				}
			}

			deleted, err := CleanLogsByDays(ctx, 30)

			if tc.cancelOnPluck > 0 {
				// 取消发生在第二批的事务里：第二批必须整批回滚，报错带 ctx 原因。
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context.Canceled, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("CleanLogsByDays failed: %v", err)
			}
			if deleted != tc.wantDeleted {
				t.Fatalf("deleted = %d, want %d", deleted, tc.wantDeleted)
			}

			// 剩余日志 = 未删的过期行 + 全部新鲜行；IO 与日志一一对应，孤儿为 0
			// 说明没有任何一处出现"IO 删了日志还在"或反过来的半删状态。
			wantRemaining := int64(tc.expired) - tc.wantDeleted + int64(tc.fresh)
			if got := countRows(t, &models.ChatLog{}, "1 = 1"); got != wantRemaining {
				t.Fatalf("remaining chat_logs = %d, want %d", got, wantRemaining)
			}
			if got := countRows(t, &models.ChatIO{}, "1 = 1"); got != wantRemaining {
				t.Fatalf("remaining chat_ios = %d, want %d", got, wantRemaining)
			}
			if got := countOrphanChatIOs(t); got != 0 {
				t.Fatalf("orphan chat_ios = %d, want 0", got)
			}

			if tc.cancelOnPluck == 0 {
				// 跑完的用例额外核对口径：过期行一个不剩，新鲜行一条不动。
				if got := countRows(t, &models.ChatLog{}, "created_at < ?", cutoff); got != 0 {
					t.Fatalf("expired chat_logs remain: %d", got)
				}
				if got := countRows(t, &models.ChatLog{}, "created_at >= ?", cutoff); got != int64(tc.fresh) {
					t.Fatalf("fresh chat_logs = %d, want %d", got, tc.fresh)
				}
			}
		})
	}
}
