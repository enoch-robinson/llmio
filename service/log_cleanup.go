package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/atopos31/llmio/models"
	"gorm.io/gorm"
)

const (
	defaultLogRetentionDays = 30
	logCleanupCheckInterval = time.Hour

	// cleanupYieldPause 批间松手后**先歇**这么久再抢第一把锁。Unlock 之后立刻
	// TryLock 是背靠背，等锁的迁移/回收还没被唤醒就又被我们顶回去，让路只剩
	// 形式。量级对齐 reclaimContinuousPause（100ms）。
	cleanupYieldPause = 100 * time.Millisecond
	// cleanupReacquireBudget 抢不回锁时的**有界**重试预算：足以覆盖迁移/回收
	// 一批的额定握锁时长（目标 2.5s，见 reclaimBatchTarget）加上抖动，量级钉在
	// busy_timeout（5s）以内。到点还抢不到就收工——清理是周期任务，剩余的行
	// 下一轮接着删，不为它无限排队。
	cleanupReacquireBudget = 4 * time.Second
)

// logCleanupBatchStart 是清理循环的**起始批量**，初值取回收的 reclaimBatchStart
// （64）：第一批无从预知库有多慢，先从慢库也付得起的量起步，快库上几批之内
// 自己涨上去（完整理由见 storage.go）。自适应本身直接复用回收的 nextBatchSize
// 与它的上下界（reclaimBatchMin/Max）——knobs 语义一致，只是单位从"页"换成了
// "行"。
//
// 是 var 只有一个原因：测试把起点调小来强制多批（见 log_cleanup_test.go）。
var logCleanupBatchStart = reclaimBatchStart

func DefaultLogCleanupPolicy() *models.LogCleanupPolicy {
	return &models.LogCleanupPolicy{
		Enabled:       false,
		RetentionDays: defaultLogRetentionDays,
	}
}

func GetLogCleanupPolicy(ctx context.Context) (*models.LogCleanupPolicy, error) {
	config, err := gorm.G[models.Config](models.DB).Where("key = ?", models.KeyLogCleanupPolicy).First(ctx)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return DefaultLogCleanupPolicy(), nil
		}
		return nil, err
	}

	if config.Value == "" {
		return DefaultLogCleanupPolicy(), nil
	}

	policy := DefaultLogCleanupPolicy()
	if err := json.Unmarshal([]byte(config.Value), policy); err != nil {
		return nil, fmt.Errorf("unmarshal log cleanup policy: %w", err)
	}
	if policy.RetentionDays <= 0 {
		policy.RetentionDays = defaultLogRetentionDays
	}
	return policy, nil
}

// CleanLogsByDays 删除 created_at 早于 days 天前的聊天日志及其关联 IO 记录，
// 返回删掉的 chat_logs 行数。
//
// 旧实现是一把 TryLock 锁到底 + 一个事务删光全部过期行。30 天累积下来那个
// 事务在 delete 日志模式下能把写锁握上**分钟级**，期间所有同步写日志的聊天
// 请求撞满 busy_timeout（5s）直接 500。现在改成**限时分批**，骨架照抄
// storage.go 的增量回收（reclaimFreePages）：
//
//   - 每批先用 LIMIT 选出这批过期 id（纯读，不进写事务），再在**同一个事务**里
//     删掉它们的 chat_ios 与 chat_logs——两者必须同生共死，半删状态比不删更糟；
//   - 批的耗时**只量事务本身**，按实测外推下一批的批量（nextBatchSize，目标
//     2.5s = busy_timeout 的一半，与回收同一份已验证的逻辑）；
//   - 批间放掉维护锁、歇一口气再抢回来，让压缩迁移/增量回收插得进来。
//
// 出错或 ctx 取消时，返回的删除数是**截至当时已提交**的量（分批保证它不含
// 半截事务）；开头抢不到维护锁仍是 ErrMaintenanceBusy。
func CleanLogsByDays(ctx context.Context, days int) (int64, error) {
	if days <= 0 {
		return 0, fmt.Errorf("days must be greater than 0")
	}

	// 与压缩迁移互斥（见 log_compress.go 的 maintenanceMu）。迁移是**每批**
	// 取放锁，所以这里等的时间上界是一批，不是一整轮。
	//
	// 用 TryLock 而不是 Lock：清理是周期任务，抢不到就下一轮，不值得为它
	// 排一个队。但**抢不到要如实报错**——手动点一下清理却静默什么都没做，
	// 比报一句"正忙"糟糕得多。
	if !maintenanceMu.TryLock() {
		return 0, ErrMaintenanceBusy
	}
	// 锁在批间会被**放掉再拿回来**，所以不能用 defer 一把梭——那样松手之后
	// 会二次 Unlock 直接 panic（同 reclaimFreePages 的 locked 标记）。
	locked := true
	defer func() {
		if locked {
			maintenanceMu.Unlock()
		}
	}()

	cutoffTime := time.Now().AddDate(0, 0, -days)
	// 这一趟的额定批量，从起点起步，之后按每批实测耗时自适应。
	batchSize := logCleanupBatchStart
	var deletedCount int64

	for {
		// 已提交的批是真删掉了；ctx 取消就把已删数如实报出去，剩余的留给
		// 下一轮（调度器对报错的处理正是"下个 tick 再来"）。
		if err := ctx.Err(); err != nil {
			return deletedCount, err
		}

		// 选批在事务外：纯读，不占写锁。选多少由上一批的实测耗时定（quota）。
		// Unscoped 对齐旧实现的裸子查询——不受软删除作用域影响。
		quota := batchSize
		var ids []uint
		if err := models.DB.WithContext(ctx).Unscoped().Model(&models.ChatLog{}).
			Where("created_at < ?", cutoffTime).
			Limit(quota).
			Pluck("id", &ids).Error; err != nil {
			return deletedCount, fmt.Errorf("select expired logs: %w", err)
		}
		if len(ids) == 0 {
			return deletedCount, nil
		}

		// 一批一个事务：先删 chat_ios 再删 chat_logs，同一事务内两者同生共死。
		batchStart := time.Now()
		var rows int64
		err := models.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Unscoped().
				Where("log_id IN ?", ids).
				Delete(&models.ChatIO{}).Error; err != nil {
				return fmt.Errorf("delete chat io: %w", err)
			}

			result := tx.Unscoped().Where("id IN ?", ids).Delete(&models.ChatLog{})
			if result.Error != nil {
				return fmt.Errorf("delete logs: %w", result.Error)
			}
			rows = result.RowsAffected
			return nil
		})
		if err != nil {
			return deletedCount, err
		}

		// 只量事务本身（同 reclaimFreePages 对 reclaimBatch 的计时方式）：
		// 自适应要的是"这个批量的代价"，混进选批/等锁的耗时就会失真。
		took := time.Since(batchStart)
		deletedCount += rows
		// 换代在前、记账在后：下一批的大小由这一批的实测耗时决定。
		batchSize = nextBatchSize(batchSize, int(rows), took)

		// 不满额定量的批是尾巴——剩下的再一次选批就能确认清完，不值得再付
		// 一次松手加等待。
		if len(ids) < quota {
			continue
		}

		// 批间让路：先放掉维护锁再歇，歇完有界地抢回来。
		maintenanceMu.Unlock()
		locked = false
		if !yieldToMaintenance(ctx) {
			if err := ctx.Err(); err != nil {
				return deletedCount, err
			}
			slog.Warn("log cleanup stopped early: lost maintenance lock between batches",
				"deleted_count", deletedCount)
			return deletedCount, ErrMaintenanceBusy
		}
		locked = true
	}
}

// yieldToMaintenance 在批与批之间给别的维护任务让路：先歇一个窗口，然后
// **有界地**重试抢锁。返回 true 表示锁已回到手里；false 表示这一趟该收工了
// ——要么 ctx 结束，要么预算内没抢到（别的维护任务连着握锁）。绝不排无限
// 队：清理是周期任务，剩余的行下一轮接着删。
func yieldToMaintenance(ctx context.Context) bool {
	// 先歇再抢：给正在排队的迁移/回收一个**先拿到**的机会。
	if !ctxSleep(ctx, cleanupYieldPause) {
		return false
	}
	deadline := time.Now().Add(cleanupReacquireBudget)
	for {
		if maintenanceMu.TryLock() {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		if !ctxSleep(ctx, min(remaining, cleanupYieldPause)) {
			return false
		}
	}
}

// ctxSleep 睡 d，ctx 取消就提前醒。返回 false 表示 ctx 已结束，不该再继续。
func ctxSleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func StartLogCleanupScheduler(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(logCleanupCheckInterval)
		defer ticker.Stop()

		lastRunDay := ""
		run := func() {
			policy, err := GetLogCleanupPolicy(ctx)
			if err != nil {
				slog.Error("load log cleanup policy failed", "error", err)
				return
			}
			if !policy.Enabled || policy.RetentionDays <= 0 {
				return
			}

			today := time.Now().Format("2006-01-02")
			if lastRunDay == today {
				return
			}

			start := time.Now()
			deletedCount, err := CleanLogsByDays(ctx, policy.RetentionDays)
			if err != nil {
				slog.Error("scheduled log cleanup failed", "retention_days", policy.RetentionDays, "error", err)
				return
			}

			lastRunDay = today
			slog.Info("scheduled log cleanup completed",
				"retention_days", policy.RetentionDays,
				"deleted_count", deletedCount,
				"duration", time.Since(start),
			)

			record := models.LogCleanupRecord{
				RetentionDays: policy.RetentionDays,
				DeletedCount:  deletedCount,
				DurationMs:    time.Since(start).Milliseconds(),
				Source:        "scheduled",
				Type:          "days",
			}
			if err := gorm.G[models.LogCleanupRecord](models.DB).Create(ctx, &record); err != nil {
				slog.Error("failed to save cleanup record", "error", err)
			}
			models.TrimLogCleanupRecords(ctx, 100)
		}

		run()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
}
