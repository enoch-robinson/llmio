package service

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

// 流式请求必须带上 stream_options.include_usage（否则上游不回 usage，token 全记 0），
// 但**只该加这一项**：客户端同时设的其它选项（如 include_obfuscation）不能被冲掉。
func TestBeforerOpenAIInjectsIncludeUsageOnly(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "没有 stream_options",
			body: `{"model":"gpt-4o","stream":true,"messages":[]}`,
		},
		{
			name: "已有其它选项",
			body: `{"model":"gpt-4o","stream":true,"stream_options":{"include_obfuscation":false},"messages":[]}`,
		},
		{
			name: "已经开着",
			body: `{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true,"include_obfuscation":false},"messages":[]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, err := BeforerOpenAI([]byte(tc.body))
			if err != nil {
				t.Fatalf("解析失败：%v", err)
			}
			raw := before.raw
			if got := gjson.GetBytes(raw, "stream_options.include_usage").Bool(); !got {
				t.Fatalf("没有注入 include_usage：%s", raw)
			}
			if bytes.Contains([]byte(tc.body), []byte("include_obfuscation")) {
				if got := gjson.GetBytes(raw, "stream_options.include_obfuscation").Bool(); got {
					t.Fatalf("客户端设的 include_obfuscation=false 被冲掉了：%s", raw)
				}
			}
		})
	}
}

// 非流式请求不该被塞 stream_options：那是流式专用字段，部分上游会直接 400。
func TestBeforerOpenAINoStreamOptionsWhenNotStreaming(t *testing.T) {
	before, err := BeforerOpenAI([]byte(`{"model":"gpt-4o","messages":[]}`))
	if err != nil {
		t.Fatalf("解析失败：%v", err)
	}
	if gjson.GetBytes(before.raw, "stream_options").Exists() {
		t.Fatalf("非流式不该注入 stream_options：%s", before.raw)
	}
}
