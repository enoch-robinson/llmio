package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/atopos31/llmio/models"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

var singleFlightGroup singleflight.Group

// authKeyCacheTTL 绕过 API 直接改库的变更最迟一个 TTL 后生效。
const authKeyCacheTTL = 15 * time.Second

var (
	authKeyCacheMu sync.RWMutex
	authKeyCache   = make(map[string]authKeyCacheEntry)
)

type authKeyCacheEntry struct {
	authKey *models.AuthKey
	at      time.Time
}

func GetAuthKey(ctx context.Context, key string) (*models.AuthKey, error) {
	authKeyCacheMu.RLock()
	if entry, ok := authKeyCache[key]; ok && time.Since(entry.at) < authKeyCacheTTL {
		authKeyCacheMu.RUnlock()
		return entry.authKey, nil
	}
	authKeyCacheMu.RUnlock()

	ch := singleFlightGroup.DoChan(key, func() (any, error) {
		authKey, err := gorm.G[models.AuthKey](models.DB).Where("key = ?", key).Where("status = ?", true).First(ctx)
		return &authKey, err
	})

	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		authKey := r.Val.(*models.AuthKey)
		authKeyCacheMu.Lock()
		authKeyCache[key] = authKeyCacheEntry{authKey: authKey, at: time.Now()}
		authKeyCacheMu.Unlock()
		return authKey, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// InvalidateAuthKeyCache 清空鉴权 Key 缓存，供 AuthKey 增删改后调用。
func InvalidateAuthKeyCache() {
	authKeyCacheMu.Lock()
	authKeyCache = make(map[string]authKeyCacheEntry)
	authKeyCacheMu.Unlock()
}

type KeyUpdateItem struct {
	Count  int
	UsedAt time.Time
}

var (
	updateCounts = make(map[uint]KeyUpdateItem)
	mu           sync.Mutex
	startOnce    sync.Once
)

func KeyUpdate(keyID uint, usedAt time.Time) {
	mu.Lock()
	updateCounts[keyID] = KeyUpdateItem{
		Count:  updateCounts[keyID].Count + 1,
		UsedAt: usedAt,
	}
	mu.Unlock()

	// 确保后台刷新协程只启动一次
	startOnce.Do(func() {
		go backgroundFlush()
	})
}

func backgroundFlush() {
	for range time.Tick(10 * time.Second) {
		ctx := context.Background()
		mu.Lock()
		for keyID, item := range updateCounts {
			if err := models.DB.Model(&models.AuthKey{}).WithContext(ctx).Where("id = ?", keyID).Updates(map[string]any{
				"usage_count":  gorm.Expr(fmt.Sprintf("usage_count + %d", item.Count)),
				"last_used_at": item.UsedAt,
			}).Error; err != nil {
				slog.Error("Failed to update auth key usage count", "error", err)
			}
		}
		updateCounts = make(map[uint]KeyUpdateItem) // 清空计数
		mu.Unlock()
	}
}
