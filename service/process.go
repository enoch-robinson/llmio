package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"sync"
	"time"

	"github.com/atopos31/llmio/models"
	"github.com/tidwall/gjson"
)

const (
	InitScannerBufferSize = 1024 * 8         // 8KB
	MaxScannerBufferSize  = 1024 * 1024 * 64 // 64MB
)

// SSE 帧前缀与结束标记。流式响应的每个 chunk 都要比一遍，用 []byte 常量免掉
// 每行的字符串分配。
var (
	sseDataPrefix  = []byte("data: ")
	sseEventPrefix = []byte("event: ")
	sseEventColon  = []byte("event:")
	sseJSONObject  = []byte("{")
	sseJSONArray   = []byte("[")
	sseDoneFrame   = []byte("[DONE]")

	// 子串预筛用的键名。见 ProcesserOpenAI 里的说明。
	sseErrorKey     = []byte(`"error"`)
	sseUsageKey     = []byte(`"usage"`)
	sseUsageMetaKey = []byte(`"usageMetadata"`)
)

// Processer 从响应流解析日志行与 token 用量；keepOutput=false（ioLog 关闭）时
// 跳过响应体累积，只保留统计所需的部分。
type Processer func(ctx context.Context, pr io.Reader, stream bool, start time.Time, keepOutput bool) (*models.ChatLog, *models.OutputUnion, error)

func ProcesserOpenAI(ctx context.Context, pr io.Reader, stream bool, start time.Time, keepOutput bool) (*models.ChatLog, *models.OutputUnion, error) {
	// 首字时延
	var firstChunkTime time.Duration
	var once sync.Once

	var usageStr string
	var output models.OutputUnion
	var size int

	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 0, InitScannerBufferSize), MaxScannerBufferSize)
	for chunk, chunkSize := range ScannerToken(scanner) {
		size += chunkSize
		once.Do(func() {
			firstChunkTime = time.Since(start)
		})
		if !stream {
			if keepOutput {
				output.OfString = string(chunk)
			}
			usageStr = gjson.GetBytes(chunk, "usage").String()
			break
		}
		chunk = bytes.TrimPrefix(chunk, sseDataPrefix)
		if bytes.Equal(chunk, sseDoneFrame) {
			break
		}
		// 每个 chunk 都要问两件事：有没有 error、有没有 usage。gjson 每次都要从头
		// 扫一遍整个对象（180 字节的 chunk 实测 ~114 ns/次），而绝大多数 chunk 两个
		// 键都没有——白白扫两遍。先用 bytes.Contains 做子串预筛，命中才解析：
		// 键在 JSON 里一定以 "error" / "usage" 的形式出现，所以不会漏；值里恰好
		// 出现这两个词只会多一次无害的扫描。
		//
		// 流式过程中错误
		if bytes.Contains(chunk, sseErrorKey) {
			if errStr := gjson.GetBytes(chunk, "error"); errStr.Exists() {
				return nil, nil, errors.New(errStr.String())
			}
		}
		if keepOutput {
			output.OfStringArray = append(output.OfStringArray, string(chunk))
		}

		// OpenAI reports a final snapshot; compatible providers may repeat cumulative
		// snapshots. Keep the latest explicit total, including zero, and ignore null.
		if bytes.Contains(chunk, sseUsageKey) {
			usage := gjson.GetBytes(chunk, "usage")
			if usage.IsObject() && usage.Get("total_tokens").Exists() {
				usageStr = usage.String()
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	// token用量
	var openaiUsage models.Usage
	usage := []byte(usageStr)
	if json.Valid(usage) {
		if err := json.Unmarshal(usage, &openaiUsage); err != nil {
			return nil, nil, err
		}
	}

	chunkTime := time.Since(start) - firstChunkTime

	return &models.ChatLog{
		FirstChunkTime: firstChunkTime,
		ChunkTime:      chunkTime,
		Usage:          openaiUsage,
		Tps:            float64(openaiUsage.CompletionTokens) / time.Since(start).Seconds(),
		Size:           size,
	}, &output, nil
}

type OpenAIResUsage struct {
	InputTokens        int64              `json:"input_tokens"`
	OutputTokens       int64              `json:"output_tokens"`
	TotalTokens        int64              `json:"total_tokens"`
	InputTokensDetails InputTokensDetails `json:"input_tokens_details"`
}

type InputTokensDetails struct {
	CachedTokens int64 `json:"cached_tokens"`
}

type AnthropicUsage struct {
	InputTokens              int64  `json:"input_tokens"`
	CacheCreationInputTokens int64  `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64  `json:"cache_read_input_tokens"`
	OutputTokens             int64  `json:"output_tokens"`
	ServiceTier              string `json:"service_tier"`
}

func ProcesserOpenAiRes(ctx context.Context, pr io.Reader, stream bool, start time.Time, keepOutput bool) (*models.ChatLog, *models.OutputUnion, error) {
	// 首字时延
	var firstChunkTime time.Duration
	var once sync.Once

	var usageStr string
	var output models.OutputUnion
	var size int

	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 0, InitScannerBufferSize), MaxScannerBufferSize)
	var event string
	for chunk, chunkSize := range ScannerToken(scanner) {
		size += chunkSize
		once.Do(func() {
			firstChunkTime = time.Since(start)
		})
		if !stream {
			if keepOutput {
				output.OfString = string(chunk)
			}
			usageStr = gjson.GetBytes(chunk, "usage").String()
			break
		}

		if after, ok := bytes.CutPrefix(chunk, sseEventPrefix); ok {
			event = string(after)
			continue
		}
		content := bytes.TrimPrefix(chunk, sseDataPrefix)
		if len(content) == 0 {
			continue
		}
		if keepOutput {
			output.OfStringArray = append(output.OfStringArray, string(content))
		}
		if event == "response.completed" {
			usageStr = gjson.GetBytes(content, "response.usage").String()
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	var openAIResUsage OpenAIResUsage
	usage := []byte(usageStr)
	if json.Valid(usage) {
		if err := json.Unmarshal(usage, &openAIResUsage); err != nil {
			return nil, nil, err
		}
	}

	chunkTime := time.Since(start) - firstChunkTime

	return &models.ChatLog{
		FirstChunkTime: firstChunkTime,
		ChunkTime:      chunkTime,
		Usage: models.Usage{
			PromptTokens:     openAIResUsage.InputTokens,
			CompletionTokens: openAIResUsage.OutputTokens,
			TotalTokens:      openAIResUsage.TotalTokens,
			PromptTokensDetails: models.PromptTokensDetails{
				CachedTokens: openAIResUsage.InputTokensDetails.CachedTokens,
			},
		},
		Tps:  float64(openAIResUsage.OutputTokens) / time.Since(start).Seconds(),
		Size: size,
	}, &output, nil
}

func ProcesserAnthropic(ctx context.Context, pr io.Reader, stream bool, start time.Time, keepOutput bool) (*models.ChatLog, *models.OutputUnion, error) {
	// 首字时延
	var firstChunkTime time.Duration
	var once sync.Once

	var usageStr string
	var anthropicUsage AnthropicUsage

	var output models.OutputUnion
	var size int

	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 0, InitScannerBufferSize), MaxScannerBufferSize)
	var event string
	for chunk, chunkSize := range ScannerToken(scanner) {
		size += chunkSize
		once.Do(func() {
			firstChunkTime = time.Since(start)
		})
		if !stream {
			if keepOutput {
				output.OfString = string(chunk)
			}
			usageStr = gjson.GetBytes(chunk, "usage").String()
			break
		}

		if after, ok := bytes.CutPrefix(chunk, sseEventPrefix); ok {
			event = string(after)
			continue
		}

		after, ok := bytes.CutPrefix(chunk, sseDataPrefix)
		if !ok {
			continue
		}

		if keepOutput {
			output.OfStringArray = append(output.OfStringArray, string(after))
		}
		switch event {
		case "message_start":
			usageStr = gjson.GetBytes(after, "message.usage").String()
		case "message_delta":
			usageStr = gjson.GetBytes(after, "usage").String()
		default:
			continue
		}
		// Usage updates are cumulative and may omit the input/cache fields from
		// message_start. Unmarshal into the current snapshot: absent fields stay,
		// while explicit zeros replace previous values.
		if usage := []byte(usageStr); json.Valid(usage) {
			if err := json.Unmarshal(usage, &anthropicUsage); err != nil {
				return nil, nil, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	usage := []byte(usageStr)
	if !stream && json.Valid(usage) {
		if err := json.Unmarshal(usage, &anthropicUsage); err != nil {
			return nil, nil, err
		}
	}

	chunkTime := time.Since(start) - firstChunkTime
	promptTokens := anthropicUsage.InputTokens + anthropicUsage.CacheCreationInputTokens + anthropicUsage.CacheReadInputTokens

	return &models.ChatLog{
		FirstChunkTime: firstChunkTime,
		ChunkTime:      chunkTime,
		Usage: models.Usage{
			PromptTokens:     promptTokens,
			CompletionTokens: anthropicUsage.OutputTokens,
			TotalTokens:      promptTokens + anthropicUsage.OutputTokens,
			PromptTokensDetails: models.PromptTokensDetails{
				CachedTokens: anthropicUsage.CacheReadInputTokens,
			},
		},
		Tps:  float64(anthropicUsage.OutputTokens) / time.Since(start).Seconds(),
		Size: size,
	}, &output, nil
}

func ProcesserGemini(ctx context.Context, pr io.Reader, stream bool, start time.Time, keepOutput bool) (*models.ChatLog, *models.OutputUnion, error) {
	// 首字时延
	var firstChunkTime time.Duration
	var once sync.Once

	var usageStr string
	var output models.OutputUnion
	var size int

	if !stream {
		bodyBytes, err := io.ReadAll(pr)
		if err != nil {
			return nil, nil, err
		}
		size += len(bodyBytes)
		once.Do(func() {
			firstChunkTime = time.Since(start)
		})
		if keepOutput {
			output.OfString = string(bodyBytes)
		}

		usageStr = gjson.GetBytes(bodyBytes, "usageMetadata").String()

	} else {
		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 0, InitScannerBufferSize), MaxScannerBufferSize)
		for chunk, chunkSize := range ScannerToken(scanner) {
			size += chunkSize
			once.Do(func() {
				firstChunkTime = time.Since(start)
			})

			if bytes.HasPrefix(chunk, sseEventColon) {
				continue
			}

			var payload []byte
			if after, ok := bytes.CutPrefix(chunk, sseDataPrefix); ok {
				payload = after
			} else if bytes.HasPrefix(chunk, sseJSONObject) || bytes.HasPrefix(chunk, sseJSONArray) {
				payload = chunk
			} else {
				continue
			}
			if len(payload) == 0 {
				continue
			}
			if bytes.Equal(payload, sseDoneFrame) {
				break
			}

			// 流式过程中错误
			if bytes.Contains(payload, sseErrorKey) {
				if errStr := gjson.GetBytes(payload, "error"); errStr.Exists() {
					return nil, nil, errors.New(errStr.String())
				}
			}

			if keepOutput {
				output.OfStringArray = append(output.OfStringArray, string(payload))
			}
			if bytes.Contains(payload, sseUsageMetaKey) {
				usageMetadata := gjson.GetBytes(payload, "usageMetadata")
				if usageMetadata.Exists() && usageMetadata.Get("totalTokenCount").Int() != 0 {
					usageStr = usageMetadata.String()
				}
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, nil, err
		}
	}

	var usage models.Usage
	usageMetadata := gjson.Parse(usageStr)
	if usageMetadata.Exists() {
		usage.PromptTokens = usageMetadata.Get("promptTokenCount").Int()
		usage.CompletionTokens = usageMetadata.Get("candidatesTokenCount").Int() + usageMetadata.Get("thoughtsTokenCount").Int()
		usage.TotalTokens = usageMetadata.Get("totalTokenCount").Int()
		if usage.TotalTokens == 0 {
			usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
		}
	}

	chunkTime := time.Since(start) - firstChunkTime

	return &models.ChatLog{
		FirstChunkTime: firstChunkTime,
		ChunkTime:      chunkTime,
		Usage:          usage,
		Tps:            float64(usage.CompletionTokens) / time.Since(start).Seconds(),
		Size:           size,
	}, &output, nil
}

// ScannerToken 逐行给出扫描结果与**该行在缓冲区里的字节数**。
//
// 它返回 []byte 而不是 string：bufio.Scanner.Text() 会为每一行复制一份字符串，
// 而流式响应一个 chunk 就是一行——2000 个 chunk 的响应因此多出 2000 次分配
// （实测 ~1 alloc/chunk）。代价是**返回值只在下一次 Scan 之前有效**：需要留存
// 的地方必须自己转成 string（见各 Processer 里的 append）。
func ScannerToken(reader *bufio.Scanner) iter.Seq2[[]byte, int] {
	return func(yield func([]byte, int) bool) {
		for reader.Scan() {
			chunk := reader.Bytes()
			if len(chunk) == 0 {
				continue
			}
			if !yield(chunk, len(chunk)) {
				return
			}
		}
	}
}
