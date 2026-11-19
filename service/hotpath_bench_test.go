package service

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// 这些基准盯的是"每个请求都要走一遍"的那几段：请求体解析、上游请求体拼装、
// 流式日志解析、provider 配置解析。它们与业务无关，只与请求数成正比。

func benchOpenAIBody(payloadBytes int) []byte {
	var b strings.Builder
	b.Grow(payloadBytes + 512)
	b.WriteString(`{"model":"gpt-4o","stream":true,"messages":[`)
	b.WriteString(`{"role":"system","content":"you are a helpful assistant"},`)
	b.WriteString(`{"role":"user","content":"`)
	line := "the quick brown fox jumps over the lazy dog. "
	for b.Len() < payloadBytes {
		b.WriteString(line)
	}
	b.WriteString(`"}]}`)
	return []byte(b.String())
}

func BenchmarkBeforerOpenAI(b *testing.B) {
	for _, size := range []int{8 << 10, 64 << 10} {
		body := benchOpenAIBody(size)
		b.Run(fmt.Sprintf("%dKB/未设置include_usage", size>>10), func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := BeforerOpenAI(body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// 客户端自己已经写了 include_usage 时，不该再重编码整包。
func BenchmarkBeforerOpenAIIncludeUsageSet(b *testing.B) {
	body := benchOpenAIBody(64 << 10)
	body = []byte(strings.Replace(string(body), `"stream":true`,
		`"stream":true,"stream_options":{"include_usage":true}`, 1))
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := BeforerOpenAI(body); err != nil {
			b.Fatal(err)
		}
	}
}

// 上游请求体：extra_body 为空时只做一次 session_id 删除。
func BenchmarkBuildUpstreamBody(b *testing.B) {
	body := benchOpenAIBody(64 << 10)
	b.Run("无extra无session", func(b *testing.B) {
		b.SetBytes(int64(len(body)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := buildUpstreamBody(body, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("有session", func(b *testing.B) {
		withSession := []byte(strings.Replace(string(body), `"stream":true`,
			`"stream":true,"session_id":"abc"`, 1))
		b.SetBytes(int64(len(withSession)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := buildUpstreamBody(withSession, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchOpenAIStream(chunks int) []byte {
	line := `{"id":"chatcmpl-bench","object":"chat.completion.chunk","created":1700000000,"model":"bench","choices":[{"index":0,"delta":{"content":"the quick brown fox "},"finish_reason":null}]}`
	var b strings.Builder
	b.Grow((len(line) + 8) * (chunks + 1))
	for i := 0; i < chunks; i++ {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteString("\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return []byte(b.String())
}

func BenchmarkProcesserOpenAI(b *testing.B) {
	stream := benchOpenAIStream(2000)
	ctx := context.Background()
	b.Run("keepOutput=true", func(b *testing.B) {
		b.SetBytes(int64(len(stream)))
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := ProcesserOpenAI(ctx, bytes.NewReader(stream), true, time.Now(), true); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("keepOutput=false", func(b *testing.B) {
		b.SetBytes(int64(len(stream)))
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := ProcesserOpenAI(ctx, bytes.NewReader(stream), true, time.Now(), false); err != nil {
				b.Fatal(err)
			}
		}
	})
}
