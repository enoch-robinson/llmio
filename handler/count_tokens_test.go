package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/atopos31/llmio/models"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// count_tokens 配置缓存与鉴权 Key / 自愈策略缓存同一套契约：TTL 内读缓存，
// 绕过失效钩子的写库要等显式失效。缓存是包级状态，用例首尾各失效一次防串味。
func setupCountTokensDB(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "count-tokens-handler.db")
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&models.Config{}); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}
	prev := models.DB
	models.DB = db
	InvalidateCountTokensConfig()
	t.Cleanup(func() {
		models.DB = prev
		InvalidateCountTokensConfig()
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
}

func TestCountTokensConfigCache(t *testing.T) {
	setupCountTokensDB(t)
	ctx := context.Background()

	// 配置行不存在：found=false 且不入缓存，创建后应立即可见
	if _, found, err := loadCountTokensConfig(ctx); found || err != nil {
		t.Fatalf("缺配置行应 found=false err=nil，实得 found=%v err=%v", found, err)
	}
	if err := models.SaveConfigValue(ctx, models.KeyAnthropicCountTokens, `{"base_url":"https://a.example.com","api_key":"sk-a","version":"2023-01-01"}`); err != nil {
		t.Fatalf("SaveConfigValue: %v", err)
	}
	cfg, found, err := loadCountTokensConfig(ctx)
	if !found || err != nil {
		t.Fatalf("写入后应读到配置，实得 found=%v err=%v", found, err)
	}
	if cfg.BaseURL != "https://a.example.com" || cfg.APIKey != "sk-a" || cfg.Version != "2023-01-01" {
		t.Fatalf("读到错误配置: %+v", cfg)
	}

	// 绕过 UpdateConfigByKey 的失效钩子直接改库：TTL 内仍命中旧缓存
	if err := models.SaveConfigValue(ctx, models.KeyAnthropicCountTokens, `{"base_url":"https://b.example.com","api_key":"sk-b","version":"2024-01-01"}`); err != nil {
		t.Fatalf("SaveConfigValue: %v", err)
	}
	if cfg, _, _ := loadCountTokensConfig(ctx); cfg.BaseURL != "https://a.example.com" {
		t.Fatalf("TTL 内应命中旧缓存，实得 %+v", cfg)
	}

	// 显式失效（或走 UpdateConfigByKey 的钩子）后读库：新值可见
	InvalidateCountTokensConfig()
	if cfg, _, _ := loadCountTokensConfig(ctx); cfg.BaseURL != "https://b.example.com" || cfg.APIKey != "sk-b" {
		t.Fatalf("失效后应读到新配置，实得 %+v", cfg)
	}
}

func TestCountTokensConfigParseError(t *testing.T) {
	setupCountTokensDB(t)
	ctx := context.Background()

	if err := models.SaveConfigValue(ctx, models.KeyAnthropicCountTokens, `{broken`); err != nil {
		t.Fatalf("SaveConfigValue: %v", err)
	}
	_, found, err := loadCountTokensConfig(ctx)
	if found || err == nil {
		t.Fatalf("坏 JSON 应 found=false 且报错，实得 found=%v err=%v", found, err)
	}
	var perr *parseConfigError
	if !errors.As(err, &perr) {
		t.Fatalf("错误应可解包为 parseConfigError，实得 %T", err)
	}
	// 坏配置不入缓存：修好后立即可读，不需要等 TTL
	if err := models.SaveConfigValue(ctx, models.KeyAnthropicCountTokens, `{"base_url":"https://c.example.com"}`); err != nil {
		t.Fatalf("SaveConfigValue: %v", err)
	}
	cfg, found, err := loadCountTokensConfig(ctx)
	if !found || err != nil || cfg.BaseURL != "https://c.example.com" {
		t.Fatalf("修好后应立即可读，实得 found=%v err=%v cfg=%+v", found, err, cfg)
	}
}

func TestCountTokensHandlerMissingConfig(t *testing.T) {
	setupCountTokensDB(t)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", nil)
	CountTokens(c)

	// common.NotFound 是 HTTP 200 + body code=404 的既有约定
	var env struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, w.Body.String())
	}
	if w.Code != http.StatusOK || env.Code != 404 {
		t.Fatalf("缺配置应回 code=404 envelope，实得 http=%d body=%s", w.Code, w.Body.String())
	}
}
