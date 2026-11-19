package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/atopos31/llmio/consts"
	"github.com/atopos31/llmio/models"
	"gorm.io/gorm"
)

// 路由解析缓存与另外几个 TTL 缓存共用的契约：绕过失效钩子的改库要等显式失效才能看到新值。
// 缓存是包级状态，用例首尾都失效防串味。

// seedRoute 灌一套最小可路由的 Provider + Model + ModelWithProvider，返回关联 ID。
func seedRoute(ctx context.Context, t *testing.T, modelName, providerName, providerModel string, toolCall bool) uint {
	t.Helper()
	provider := models.Provider{Name: providerName, Type: consts.StyleOpenAI, Config: `{"base_url":"http://127.0.0.1:1","api_key":"sk-test"}`}
	if err := gorm.G[models.Provider](models.DB).Create(ctx, &provider); err != nil {
		t.Fatalf("Create provider: %v", err)
	}
	model := models.Model{
		Name:     modelName,
		MaxRetry: 3,
		TimeOut:  60,
		Strategy: consts.BalancerRotor,
		Breaker:  new(true),
	}
	if err := gorm.G[models.Model](models.DB).Create(ctx, &model); err != nil {
		t.Fatalf("Create model: %v", err)
	}
	mp := models.ModelWithProvider{
		ModelID:       model.ID,
		ProviderID:    provider.ID,
		ProviderModel: providerModel,
		Status:        new(true),
		ToolCall:      new(toolCall),
		Backup:        new(false),
		Weight:        10,
	}
	if err := gorm.G[models.ModelWithProvider](models.DB).Create(ctx, &mp); err != nil {
		t.Fatalf("Create model-with-provider: %v", err)
	}
	return mp.ID
}

func TestModelRouteCache(t *testing.T) {
	models.Init(context.Background(), filepath.Join(t.TempDir(), "route-cache.db"))
	InvalidateModelRouteCache()
	t.Cleanup(InvalidateModelRouteCache)
	ctx := context.Background()

	seedRoute(ctx, t, "gpt-cache", "p-cache", "upstream-cache", true)
	before := Before{Model: "gpt-cache", toolCall: true}

	meta, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, before)
	if err != nil {
		t.Fatalf("ProvidersWithMetaBymodelsName: %v", err)
	}
	if meta.MaxRetry != 3 || meta.TimeOut != 60 || meta.Strategy != consts.BalancerRotor || !meta.Breaker {
		t.Fatalf("模型元信息应原样透传，实得 %+v", meta)
	}
	if len(meta.ProviderMap) != 1 || len(meta.WeightItems) != 1 || len(meta.ModelWithProviderMap) != 1 {
		t.Fatalf("首轮解析应拿到 1 家候选，实得 %+v", meta)
	}

	// 绕过失效钩子直接改库：TTL 内仍命中缓存，证明二次调用没打库
	if _, err := gorm.G[models.ModelWithProvider](models.DB).Where("provider_model = ?", "upstream-cache").
		Update(ctx, "status", false); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, before); err != nil {
		t.Fatalf("TTL 内应命中缓存: %v", err)
	}

	// 失效后重新查库：被停用的关联不再可用
	InvalidateModelRouteCache()
	if _, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, before); err == nil {
		t.Fatal("失效后应重新查库并报无可用 provider")
	}
}

func TestModelRouteCacheKeyIsolation(t *testing.T) {
	models.Init(context.Background(), filepath.Join(t.TempDir(), "route-cache-isolation.db"))
	InvalidateModelRouteCache()
	t.Cleanup(InvalidateModelRouteCache)
	ctx := context.Background()

	seedRoute(ctx, t, "gpt-iso", "p-iso", "upstream-tc", true)
	// 同一模型下再挂一条不带 toolCall 能力的关联：两个 key 各自解析出不同的关联
	modelRow, err := gorm.G[models.Model](models.DB).Where("name = ?", "gpt-iso").First(ctx)
	if err != nil {
		t.Fatalf("First model: %v", err)
	}
	providerRow, err := gorm.G[models.Provider](models.DB).Where("name = ?", "p-iso").First(ctx)
	if err != nil {
		t.Fatalf("First provider: %v", err)
	}
	plain := models.ModelWithProvider{
		ModelID:       modelRow.ID,
		ProviderID:    providerRow.ID,
		ProviderModel: "upstream-plain",
		Status:        new(true),
		ToolCall:      new(false),
		Backup:        new(false),
		Weight:        10,
	}
	if err := gorm.G[models.ModelWithProvider](models.DB).Create(ctx, &plain); err != nil {
		t.Fatalf("Create model-with-provider: %v", err)
	}

	tcKey := Before{Model: "gpt-iso", toolCall: true}
	plainKey := Before{Model: "gpt-iso", toolCall: false}

	metaTc, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, tcKey)
	if err != nil {
		t.Fatalf("tcKey 解析: %v", err)
	}
	for id, mp := range metaTc.ModelWithProviderMap {
		if mp.ProviderModel != "upstream-tc" {
			t.Fatalf("tcKey 只应拿到 toolCall 关联，ID %d 实得 %s", id, mp.ProviderModel)
		}
	}
	metaPlain, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, plainKey)
	if err != nil {
		t.Fatalf("plainKey 解析: %v", err)
	}
	// plainKey（toolCall=false）不加能力过滤，应拿到全部两条关联
	if len(metaPlain.ModelWithProviderMap) != 2 {
		t.Fatalf("plainKey 应拿到全部关联，实得 %+v", metaPlain.ModelWithProviderMap)
	}

	// 绕过失效钩子直接删掉 toolCall 关联
	if _, err := gorm.G[models.ModelWithProvider](models.DB).Where("provider_model = ?", "upstream-tc").
		Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// TTL 内两个 key 各自命中各自的缓存：不同 key 互不污染
	metaTc2, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, tcKey)
	if err != nil || len(metaTc2.ModelWithProviderMap) != 1 {
		t.Fatalf("tcKey 应命中缓存，err=%v meta=%+v", err, metaTc2)
	}
	metaPlain2, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, plainKey)
	if err != nil || len(metaPlain2.ModelWithProviderMap) != 2 {
		t.Fatalf("plainKey 应命中缓存，err=%v meta=%+v", err, metaPlain2)
	}

	// 失效后重新查库：tcKey 已无候选，plainKey 不受牵连、只剩普通关联
	InvalidateModelRouteCache()
	if _, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, tcKey); err == nil {
		t.Fatal("失效后 tcKey 应重新查库并报无可用 provider")
	}
	metaPlain3, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, plainKey)
	if err != nil || len(metaPlain3.ModelWithProviderMap) != 1 {
		t.Fatalf("失效后 plainKey 应只剩普通关联，err=%v meta=%+v", err, metaPlain3)
	}
	for _, mp := range metaPlain3.ModelWithProviderMap {
		if mp.ProviderModel != "upstream-plain" {
			t.Fatalf("失效后 plainKey 不应再含已删除的关联，实得 %s", mp.ProviderModel)
		}
	}
}

func TestModelRouteCacheNotFound(t *testing.T) {
	models.Init(context.Background(), filepath.Join(t.TempDir(), "route-cache-notfound.db"))
	InvalidateModelRouteCache()
	t.Cleanup(InvalidateModelRouteCache)
	ctx := context.Background()

	before := Before{Model: "gpt-missing"}
	if _, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, before); err == nil {
		t.Fatal("模型不存在应报错")
	}
	// 负结果不缓存：二次调用仍走查库、仍报错
	if _, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, before); err == nil {
		t.Fatal("负结果不缓存：二次调用仍应报错")
	}
	// 每次未命中都各落一条失败日志，行为与加缓存前一致
	failed, err := gorm.G[models.ChatLog](models.DB).Where("name = ?", "gpt-missing").Count(ctx, "id")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if failed != 2 {
		t.Fatalf("两次未命中应各落一条失败日志，实得 %d 条", failed)
	}

	// 直接补建路由（测试不走 handler，天然绕过失效钩子）后不失效也能立刻解析成功：
	// 若负结果曾被缓存，这里仍会报错
	seedRoute(ctx, t, "gpt-missing", "p-missing", "upstream-missing", true)
	meta, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, before)
	if err != nil {
		t.Fatalf("补建路由后应解析成功: %v", err)
	}
	if len(meta.ProviderMap) != 1 {
		t.Fatalf("补建后应拿到 1 家候选，实得 %+v", meta)
	}
}

// 深拷贝契约：缓存母本绝不能被消费方写穿。Lottery balancer 会按引用持有 WeightItems
// 并在转发路径上 Delete/Reduce，这里模拟该行为，确认拷贝件被改写后母本与缓存不受影响。
func TestModelRouteCacheDeepCopy(t *testing.T) {
	models.Init(context.Background(), filepath.Join(t.TempDir(), "route-cache-copy.db"))
	InvalidateModelRouteCache()
	t.Cleanup(InvalidateModelRouteCache)
	ctx := context.Background()

	seedRoute(ctx, t, "gpt-copy", "p-copy", "upstream-copy", true)
	before := Before{Model: "gpt-copy", toolCall: true}

	first, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, before)
	if err != nil {
		t.Fatalf("ProvidersWithMetaBymodelsName: %v", err)
	}
	// 消费方（balancer）在拷贝件上淘汰候选
	for id := range first.WeightItems {
		delete(first.WeightItems, id)
	}
	first.ProviderMap[999999] = models.Provider{Name: "poison"}

	second, err := ProvidersWithMetaBymodelsName(ctx, consts.StyleOpenAI, before)
	if err != nil {
		t.Fatalf("二次解析: %v", err)
	}
	if len(second.WeightItems) != 1 || len(second.ProviderMap) != 1 {
		t.Fatalf("缓存的母本不应被拷贝件写穿，实得 WeightItems=%v ProviderMap=%v", second.WeightItems, second.ProviderMap)
	}
}
