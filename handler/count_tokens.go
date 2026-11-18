package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/atopos31/llmio/common"
	"github.com/atopos31/llmio/models"
	"github.com/atopos31/llmio/providers"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// countTokensConfigTTL 30s：配置天级改一次，写后失效（UpdateConfigByKey）。
const countTokensConfigTTL = 30 * time.Second

var (
	countTokensMu    sync.RWMutex
	countTokensCfg   models.AnthropicCountTokens
	countTokensAt    time.Time
	countTokensReady bool
)

type parseConfigError struct{ err error }

func (e *parseConfigError) Error() string { return e.err.Error() }
func (e *parseConfigError) Unwrap() error { return e.err }

// loadCountTokensConfig found=false 表示配置行不存在，调用方按 NotFound 响应；
// 缺行不入缓存，创建后立即可见。
func loadCountTokensConfig(ctx context.Context) (models.AnthropicCountTokens, bool, error) {
	countTokensMu.RLock()
	if countTokensReady && time.Since(countTokensAt) < countTokensConfigTTL {
		cfg := countTokensCfg
		countTokensMu.RUnlock()
		return cfg, true, nil
	}
	countTokensMu.RUnlock()

	config, err := gorm.G[models.Config](models.DB).Where("key = ?", models.KeyAnthropicCountTokens).First(ctx)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.AnthropicCountTokens{}, false, nil
		}
		return models.AnthropicCountTokens{}, false, err
	}
	var cfg models.AnthropicCountTokens
	if err := json.Unmarshal([]byte(config.Value), &cfg); err != nil {
		return models.AnthropicCountTokens{}, false, &parseConfigError{err}
	}

	countTokensMu.Lock()
	countTokensCfg = cfg
	countTokensAt = time.Now()
	countTokensReady = true
	countTokensMu.Unlock()
	return cfg, true, nil
}

func InvalidateCountTokensConfig() {
	countTokensMu.Lock()
	countTokensCfg = models.AnthropicCountTokens{}
	countTokensAt = time.Time{}
	countTokensReady = false
	countTokensMu.Unlock()
}

var countTokensHTTPClient = &http.Client{Timeout: 30 * time.Second}

func CountTokens(c *gin.Context) {
	ctx := c.Request.Context()

	config, found, err := loadCountTokensConfig(ctx)
	if err != nil {
		var perr *parseConfigError
		if errors.As(err, &perr) {
			common.InternalServerError(c, "Failed to parse Anthropic count tokens config: "+err.Error())
		} else {
			common.InternalServerError(c, "Failed to retrieve Anthropic count tokens config: "+err.Error())
		}
		return
	}
	if !found {
		common.NotFound(c, "Anthropic count tokens config not found")
		return
	}
	anthropic := providers.Anthropic{
		BaseURL: config.BaseURL,
		APIKey:  config.APIKey,
		Version: config.Version,
	}

	req, err := anthropic.BuildCountTokensReq(ctx, c.Request.Header, c.Request.Body)
	if err != nil {
		common.InternalServerError(c, "Failed to create request: "+err.Error())
		return
	}

	res, err := countTokensHTTPClient.Do(req)
	if err != nil {
		common.InternalServerError(c, "Failed to send request: "+err.Error())
		return
	}
	defer res.Body.Close()

	c.Status(res.StatusCode)

	for k, values := range res.Header {
		for _, value := range values {
			c.Writer.Header().Add(k, value)
		}
	}
	c.Writer.Flush()

	if _, err := io.Copy(c.Writer, res.Body); err != nil {
		common.InternalServerError(c, "Failed to read response: "+err.Error())
		return
	}
}

const testBody = `{
    	"model": "claude-sonnet-4-5",
    	"messages": [
      		{
        		"role": "user", 
        		"content": "Write a one-sentence bedtime story about a unicorn."
      		}
    	],
		"system": "You are a helpful assistant."
 	}`

type CountTokensResponse struct {
	InputTokens int64 `json:"input_tokens"`
}

func TestCountTokens(c *gin.Context) {
	ctx := c.Request.Context()

	config, found, err := loadCountTokensConfig(ctx)
	if err != nil {
		common.InternalServerError(c, "Failed to retrieve Anthropic count tokens config: "+err.Error())
		return
	}
	if !found {
		common.NotFound(c, "Anthropic count tokens config not found")
		return
	}
	anthropic := providers.Anthropic{
		BaseURL: config.BaseURL,
		APIKey:  config.APIKey,
		Version: config.Version,
	}

	req, err := anthropic.BuildCountTokensReq(ctx, nil, strings.NewReader(testBody))
	if err != nil {
		common.InternalServerError(c, "Failed to create request: "+err.Error())
		return
	}

	res, err := countTokensHTTPClient.Do(req)
	if err != nil {
		common.InternalServerError(c, "Failed to send request: "+err.Error())
		return
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		content, _ := io.ReadAll(res.Body)
		common.ErrorWithHttpStatus(c, http.StatusOK, res.StatusCode, "Failed to send request. status: "+res.Status+" content: "+string(content))
		return
	}
	var countTokensRes CountTokensResponse
	if err := json.NewDecoder(res.Body).Decode(&countTokensRes); err != nil {
		common.ErrorWithHttpStatus(c, http.StatusOK, res.StatusCode, "Failed to parse response: "+err.Error())
		return
	}

	if countTokensRes.InputTokens == 0 {
		common.ErrorWithHttpStatus(c, http.StatusOK, res.StatusCode, "Counted 0 input tokens, something went wrong")
		return
	}

	common.Success(c, "Anthropic count tokens test successful")
}
