package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/atopos31/llmio/models"
	"gorm.io/gorm"
)

// 两个 TTL 缓存（鉴权 Key、自愈策略）共用的行为契约：TTL 内读缓存；绕过带
// 失效钩子的 API 写路径直接改库时，要等显式失效才能看到新值。
// 缓存是包级状态，用例首尾都做一次失效，避免跨用例串味。

func TestSelfHealPolicyCache(t *testing.T) {
	models.Init(context.Background(), filepath.Join(t.TempDir(), "selfheal-cache.db"))
	InvalidateSelfHealPolicy()
	t.Cleanup(InvalidateSelfHealPolicy)
	ctx := context.Background()

	// 缺行时回默认策略，并且这个结果进缓存
	if got := GetSelfHealPolicy(ctx); !got.Enabled || got.MaxDrops != SelfHealMaxDrops {
		t.Fatalf("缺行应回默认策略，实得 %+v", got)
	}
	if err := models.SaveConfigValue(ctx, models.KeySelfHealPolicy, `{"enabled":false,"max_drops":1}`); err != nil {
		t.Fatalf("SaveConfigValue: %v", err)
	}
	// 直接改库绕过了失效钩子：TTL 内仍读缓存
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
	// 直接改库禁用，绕过 handler 的失效钩子：TTL 内仍命中缓存
	if _, err := gorm.G[models.AuthKey](models.DB).Where("key = ?", "sk-cache-test").Update(ctx, "status", false); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, err := GetAuthKey(ctx, "sk-cache-test"); err != nil {
		t.Fatalf("TTL 内应命中缓存: %v", err)
	}
	// 显式失效后读库：禁用的 Key 不再可用
	InvalidateAuthKeyCache()
	if _, err := GetAuthKey(ctx, "sk-cache-test"); err == nil {
		t.Fatal("失效后禁用的 Key 应查不到")
	}
}
