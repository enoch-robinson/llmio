package service

import (
	"bytes"
	"testing"
)

// 没有 session_id 的请求体必须**原样**交给上游：不重编码、不复制。
// 判据取"同一块底层数组"——sjson 一旦被调用就会返回新分配的切片，指针必然变。
func TestBuildUpstreamBodyLeavesBodyUntouchedWithoutSessionID(t *testing.T) {
	raw := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`)
	out, err := buildUpstreamBody(raw, nil)
	if err != nil {
		t.Fatalf("拼装失败：%v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Fatalf("请求体被改了：\n got %s\nwant %s", out, raw)
	}
	if len(out) > 0 && &out[0] != &raw[0] {
		t.Fatal("没有 session_id 却重编码了整个 body（多了一次整包拷贝）")
	}
}

// 有 session_id 时仍然要删干净——那条契约由 chat_test.go 的
// TestBuildUpstreamBodyDeletesSessionID 盯着（含 extra_body 一起验），这里不重复。
