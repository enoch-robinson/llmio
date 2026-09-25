package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/atopos31/llmio/consts"
	"github.com/atopos31/llmio/models"
	"gorm.io/gorm"
)

// TestDoWithHeaderTimeoutFires 验证:响应头超过预算未到达时返回 errResponseHeaderTimeout,
// 而不是与客户端断开难以区分的 context.Canceled。
func TestDoWithHeaderTimeoutFires(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond) // 迟迟不回响应头,模拟慢首字供应商
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = doWithHeaderTimeout(context.Background(), &http.Client{}, req, 100*time.Millisecond)
	if !errors.Is(err, errResponseHeaderTimeout) {
		t.Fatalf("err = %v, want errResponseHeaderTimeout", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal("header timeout must not be reported as context.Canceled")
	}
}

// TestDoWithHeaderTimeoutBodyNotCutOff 验证核心正确性:
// 头阶段超时只约束响应头等待,不限制流式 body 的传输时长。
// 服务端先立即返回响应头,随后以慢于头超时的节奏持续输出 SSE,
// 若超时错误地作用于整个响应,body 会被截断。
func TestDoWithHeaderTimeoutBodyNotCutOff(t *testing.T) {
	const chunks = 8
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		for i := 0; i < chunks; i++ {
			time.Sleep(80 * time.Millisecond)
			_, _ = io.WriteString(w, "data: chunk\n\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, cancel, err := doWithHeaderTimeout(context.Background(), &http.Client{}, req, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer cancel(nil)
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if got := strings.Count(string(body), "data: chunk"); got != chunks {
		t.Fatalf("streamed %d/%d chunks, body was cut off by header timeout", got, chunks)
	}
}

// TestDoWithHeaderTimeoutZeroMeansNoLimit 验证:TimeOut=0 时保持原语义(不限制头等待时间)。
func TestDoWithHeaderTimeoutZeroMeansNoLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(250 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, cancel, err := doWithHeaderTimeout(context.Background(), &http.Client{}, req, 0)
	if err != nil {
		t.Fatalf("do with headerTimeout=0: %v", err)
	}
	defer cancel(nil)
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
}

// TestDoWithHeaderTimeoutParentCancelNotMisreported 验证:客户端主动断开
// 不会被误报为 errResponseHeaderTimeout(保证日志与重试策略的准确性)。
func TestDoWithHeaderTimeoutParentCancelNotMisreported(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-block // 挂住,永不返回响应头
	}))
	defer srv.Close()
	defer close(block) // 先于 srv.Close 执行,释放 handler

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 头超时设为 1h,确保触发的一定是父 ctx 取消
	_, _, err = doWithHeaderTimeout(ctx, &http.Client{}, req, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if errors.Is(err, errResponseHeaderTimeout) {
		t.Fatal("parent cancel must not be reported as errResponseHeaderTimeout")
	}
}

// newBalanceChatFixture 构造指向测试服务器的 BalanceChat 入参。
func newBalanceChatFixture(t *testing.T, providerID uint, baseURL string) (Before, ProvidersWithMeta) {
	t.Helper()
	before := Before{
		Model:  "test-model",
		Stream: true,
		raw:    []byte(`{"model":"test-model","messages":[]}`),
	}
	meta := ProvidersWithMeta{
		ModelWithProviderMap: map[uint]models.ModelWithProvider{
			providerID: {Model: gorm.Model{ID: providerID}, ProviderID: providerID, ProviderModel: "provider-model"},
		},
		WeightItems: map[uint]int{providerID: 1},
		ProviderMap: map[uint]models.Provider{
			providerID: {
				Name:   fmt.Sprintf("provider-%d", providerID),
				Type:   string(consts.StyleOpenAI),
				Config: fmt.Sprintf(`{"base_url":%q,"api_key":"test-key"}`, baseURL),
			},
		},
		MaxRetry: 3,
		TimeOut:  5,
	}
	return before, meta
}

// TestBalanceChatStreamEndToEnd 集成验证:BalanceChat -> doWithHeaderTimeout -> SSE
// 全链路可建立流并完整转发慢速 body,且返回的 cancel 在 body 消费后可安全调用。
func TestBalanceChatStreamEndToEnd(t *testing.T) {
	const chunks = 5
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		for i := 0; i < chunks; i++ {
			time.Sleep(40 * time.Millisecond)
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"c\"}}]}\n\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	before, meta := newBalanceChatFixture(t, 7, srv.URL+"/v1")

	res, log, cancel, err := BalanceChat(context.Background(), time.Now(), string(consts.StyleOpenAI), before, meta, models.ReqMeta{})
	if err != nil {
		t.Fatalf("BalanceChat: %v", err)
	}
	defer cancel(nil)
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if got := strings.Count(string(body), "data: {"); got != chunks {
		t.Fatalf("forwarded %d/%d chunks", got, chunks)
	}
	if !strings.Contains(string(body), "data: [DONE]") {
		t.Error("body missing [DONE] terminator")
	}
	if log.ProviderName != "provider-7" || log.Retry != 0 {
		t.Errorf("log = provider:%s retry:%d, want provider-7 retry:0", log.ProviderName, log.Retry)
	}
}

// TestBalanceChatFailoverOnFastFailure 集成验证:供应商快速失败(连接拒绝)时
// 重试机制切换到健康供应商,并写入失败日志。
func TestBalanceChatFailoverOnFastFailure(t *testing.T) {
	models.Init(context.Background(), filepath.Join(t.TempDir(), "llmio-test.db"))

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer good.Close()

	// 构造一个已关闭的地址,制造快速失败
	bad := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	badURL := bad.URL
	bad.Close()

	before := Before{Model: "test-model", Stream: true, raw: []byte(`{"model":"test-model"}`)}
	// rotor 策略按权重降序确定性轮转:bad(权重 2)必先被选中,
	// 快速失败后 Delete 移除,第二次 Pop 命中 good。
	meta := ProvidersWithMeta{
		ModelWithProviderMap: map[uint]models.ModelWithProvider{
			11: {Model: gorm.Model{ID: 11}, ProviderID: 11, ProviderModel: "pm-bad"},
			22: {Model: gorm.Model{ID: 22}, ProviderID: 22, ProviderModel: "pm-good"},
		},
		WeightItems: map[uint]int{11: 2, 22: 1},
		Strategy:    consts.BalancerRotor,
		ProviderMap: map[uint]models.Provider{
			11: {Name: "bad-provider", Type: string(consts.StyleOpenAI), Config: fmt.Sprintf(`{"base_url":%q,"api_key":"k"}`, badURL+"/v1")},
			22: {Name: "good-provider", Type: string(consts.StyleOpenAI), Config: fmt.Sprintf(`{"base_url":%q,"api_key":"k"}`, good.URL+"/v1")},
		},
		MaxRetry: 3,
		TimeOut:  5,
	}

	res, log, cancel, err := BalanceChat(context.Background(), time.Now(), string(consts.StyleOpenAI), before, meta, models.ReqMeta{})
	if err != nil {
		t.Fatalf("BalanceChat: %v", err)
	}
	defer cancel(nil)
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if log.ProviderName != "good-provider" || log.Retry != 1 {
		t.Errorf("log = provider:%s retry:%d, want good-provider retry:1 (bad provider fails fast first)", log.ProviderName, log.Retry)
	}
	if !strings.Contains(res.Request.URL.String(), good.URL) {
		t.Errorf("final request host = %s, want good provider %s", res.Request.URL, good.URL)
	}

	// 失败日志最终异步落库
	deadline := time.Now().Add(2 * time.Second)
	for {
		var logs []models.ChatLog
		logs, err = gorm.G[models.ChatLog](models.DB).Where("provider_name = ?", "bad-provider").Find(context.Background())
		if err == nil && len(logs) > 0 {
			if !strings.Contains(logs[0].Error, "connection refused") {
				t.Errorf("retry log error = %q, want connection refused", logs[0].Error)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retry log for bad-provider not found: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
