package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/atopos31/llmio/models"
	"gorm.io/gorm"
)

// 两个 TTL 缓存共用的契约：绕过失效钩子的改库要等显式失效才能看到新值。
// 缓存是包级状态，用例首尾都失效防串味。

func TestSelfHealPolicyCache(t *testing.T) {
	models.Init(context.Background(), filepath.Join(t.TempDir(), "selfheal-cache.db"))
	InvalidateSelfHealPolicy()
	t.Cleanup(InvalidateSelfHealPolicy)
	ctx := context.Background()

	if got := GetSelfHealPolicy(ctx); !got.Enabled || got.MaxDrops != SelfHealMaxDrops {
		t.Fatalf("缺行应回默认策略，实得 %+v", got)
	}
	if err := models.SaveConfigValue(ctx, models.KeySelfHealPolicy, `{"enabled":false,"max_drops":1}`); err != nil {
		t.Fatalf("SaveConfigValue: %v", err)
	}
	// 绕过失效钩子直接改库：TTL 内仍读缓存
	if got := GetSelfHealPolicy(ctx); !got.Enabled || got.MaxDrops != SelfHealMaxDrops {
		t.Fatalf("TTL 内应返回缓存的默认策略，实得 %+v", got)
	}
	InvalidateSelfHealPolicy()
	if got := GetSelfHealPolicy(ctx); got.Enabled || got.MaxDrops != 1 {
		t.Fatalf("失效后应读到新策略，实得 %+v", got)
	}
}

func TestAuthKeyCache(t *testing.T) {
	models.Init(context.Background(), filepath.Join(t.TempDir(), "authkey-cache.db"))
	InvalidateAuthKeyCache()
	t.Cleanup(InvalidateAuthKeyCache)
	ctx := context.Background()

	authKey := models.AuthKey{Name: "cache-test", Key: "sk-cache-test", Status: new(true)}
	if err := gorm.G[models.AuthKey](models.DB).Create(ctx, &authKey); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := GetAuthKey(ctx, "sk-cache-test"); err != nil {
		t.Fatalf("GetAuthKey: %v", err)
	}
	// 绕过失效钩子直接改库：TTL 内仍命中缓存
	if _, err := gorm.G[models.AuthKey](models.DB).Where("key = ?", "sk-cache-test").Update(ctx, "status", false); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, err := GetAuthKey(ctx, "sk-cache-test"); err != nil {
		t.Fatalf("TTL 内应命中缓存: %v", err)
	}
	InvalidateAuthKeyCache()
	if _, err := GetAuthKey(ctx, "sk-cache-test"); err == nil {
		t.Fatal("失效后禁用的 Key 应查不到")
	}
}
