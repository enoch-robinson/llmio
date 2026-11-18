package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/atopos31/llmio/consts"
	"github.com/atopos31/llmio/models"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// setupErrorBodyDB 与 setupErrstageDB 同构，但 Provider.ErrorMatcher 可配。
func setupErrorBodyDB(t *testing.T, baseURL, errorMatcher string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "errorbody-handler.db")
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&models.Provider{}, &models.Model{}, &models.ModelWithProvider{}, &models.ChatLog{}, &models.Config{}); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}

	provider := models.Provider{
		Name:         "prov-errorbody",
		Type:         consts.StyleOpenAI,
		Config:       `{"base_url":"` + baseURL + `","api_key":"sk-test"}`,
		ErrorMatcher: errorMatcher,
	}
	if err := db.Create(&provider).Error; err != nil {
		t.Fatalf("create provider: %v", err)
	}
	model := models.Model{Name: "m-test", MaxRetry: 1, TimeOut: 5, Strategy: consts.BalancerDefault}
	if err := db.Create(&model).Error; err != nil {
		t.Fatalf("create model: %v", err)
	}
	enabled := true
	association := models.ModelWithProvider{
		ModelID:       model.ID,
		ProviderID:    provider.ID,
		ProviderModel: "test-model",
		Status:        &enabled,
		Weight:        1,
	}
	if err := db.Create(&association).Error; err != nil {
		t.Fatalf("create association: %v", err)
	}

	prev := models.DB
	models.DB = db
	t.Cleanup(func() {
		models.DB = prev
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
}

// largeCompletionBody 构造 content 为 contentLen 字节的合法 OpenAI 非流式响应。
func largeCompletionBody(contentLen int) string {
	return `{"id":"1","object":"chat.completion","model":"test-model","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"` +
		strings.Repeat("x", contentLen) +
		`"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
}

func doChatRequest(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	ctx := context.WithValue(context.Background(), consts.ContextKeyAllowAllModel, true)
	body := `{"model":"m-test","messages":[{"role":"user","content":"hi"}]}`
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")

	ChatCompletionsHandler(c)
	return w
}

// 样本落在 64KB 采样窗内的大响应（200KB）必须仍被 ErrorMatcher 识别为错误，
// 且落库 error 携带样本与截断后的 body。
func TestErrorMatcherMatchesSampleInPrefixOfLargeBody(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		body := strings.Replace(largeCompletionBody(200_000), `"content":"x`,
			`"content":"quota exceeded x`, 1)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(upstream.Close)

	setupErrorBodyDB(t, upstream.URL, "quota exceeded")
	w := doChatRequest(t)
	if w.Code < 500 {
		t.Fatalf("样本命中应使请求失败（5xx），实得 %d", w.Code)
	}

	waitFor(t, "error 日志落库", func() bool {
		n, err := gorm.G[models.ChatLog](models.DB).Where("status = ?", consts.StatusError).Count(context.Background(), "id")
		return err == nil && n > 0
	})
	log, err := gorm.G[models.ChatLog](models.DB).Where("status = ?", consts.StatusError).First(context.Background())
	if err != nil {
		t.Fatalf("query error log: %v", err)
	}
	if !strings.Contains(log.Error, "response matched provider error sample") || !strings.Contains(log.Error, "quota exceeded") {
		t.Fatalf("落库 error 应包含样本命中信息，实得前 200 字节: %.200q", log.Error)
	}
}

// 样本只在 64KB 采样窗之外出现时不应命中（只采样前缀的全部意义），
// 且响应体要逐字节无损转发到客户端——这同时钉住 MultiReader 拼接
// 与原 body Close 链两条回归线。
func TestErrorMatcherIgnoresSampleBeyondPrefixAndForwardsBodyIntact(t *testing.T) {
	const (
		padBefore = 70_000
		padAfter  = 10_000
	)
	upstreamBody := strings.Replace(largeCompletionBody(padBefore+padAfter),
		// content 由连续 x 组成，把样本塞进 70KB 之后：完整读才会命中，采样读不会
		strings.Repeat("x", padBefore), strings.Repeat("x", padBefore-1)+"quota exceeded", 1)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	t.Cleanup(upstream.Close)

	setupErrorBodyDB(t, upstream.URL, "quota exceeded")
	w := doChatRequest(t)

	if w.Code != http.StatusOK {
		t.Fatalf("越界样本不应命中，应原样转发 200，实得 %d body 前 100 字节: %.100q", w.Code, w.Body.String())
	}
	if !bytes.Equal(w.Body.Bytes(), []byte(upstreamBody)) {
		t.Fatalf("转发体与上游体不一致：got %d bytes, want %d bytes", w.Body.Len(), len(upstreamBody))
	}
	// RecordLog 是后台协程且用包级 DB：等它落库完成再让 Cleanup 换回 DB，
	// 否则收尾时 panic
	waitFor(t, "success 日志落库", func() bool {
		n, err := gorm.G[models.ChatLog](models.DB).Where("status = ?", consts.StatusSuccess).Count(context.Background(), "id")
		return err == nil && n > 0
	})
}

// 非 200 错误体闸在 1MB：上游返回 5MB 错误页时，落库 error 里的 body 部分
// 不应超过闸门（旧实现会把 5MB 全量塞进日志）。
func TestNon200ErrorBodyCappedAtLimit(t *testing.T) {
	const hugeBodyLen = 5 << 20
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(strings.Repeat("e", hugeBodyLen)))
	}))
	t.Cleanup(upstream.Close)

	setupErrorBodyDB(t, upstream.URL, "")
	w := doChatRequest(t)
	if w.Code < 500 {
		t.Fatalf("非 200 应使请求失败（5xx），实得 %d", w.Code)
	}

	waitFor(t, "error 日志落库", func() bool {
		n, err := gorm.G[models.ChatLog](models.DB).Where("status = ?", consts.StatusError).Count(context.Background(), "id")
		return err == nil && n > 0
	})
	log, err := gorm.G[models.ChatLog](models.DB).Where("status = ?", consts.StatusError).First(context.Background())
	if err != nil {
		t.Fatalf("query error log: %v", err)
	}
	if !strings.HasPrefix(log.Error, "status: 429, body: ") {
		t.Fatalf("落库 error 应以状态与 body 前缀开头，实得前 40 字节: %.40q", log.Error)
	}
	// service.errorBodyLimit（1MB）加余量给错误链包装；用字面量避免为测试导出常量
	if len(log.Error) > 1<<20+512 {
		t.Fatalf("落库 error 应被闸在 1MB 内，实得 %d 字节", len(log.Error))
	}
}
