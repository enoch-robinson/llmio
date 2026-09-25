package providers

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetClientCachesPerProxy(t *testing.T) {
	a1 := GetClient("")
	a2 := GetClient("")
	if a1 != a2 {
		t.Fatal("same proxy should return the same cached client")
	}

	b1 := GetClient("http://127.0.0.1:1")
	if b1 == a1 {
		t.Fatal("different proxy should return a different client")
	}
	b2 := GetClient("http://127.0.0.1:1")
	if b1 != b2 {
		t.Fatal("same proxy should return the same cached client")
	}
}

func TestGetClientTransportConfig(t *testing.T) {
	client := GetClient("")
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 = false, want true")
	}
	if tr.MaxIdleConnsPerHost != 100 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 100 (stdlib default 2 causes TLS churn)", tr.MaxIdleConnsPerHost)
	}
	if tr.ResponseHeaderTimeout != 0 {
		t.Errorf("ResponseHeaderTimeout = %s, want 0 (header timeout is now per-request)", tr.ResponseHeaderTimeout)
	}
}

func TestConfigureHTTP2Keepalive(t *testing.T) {
	tr := &http.Transport{ForceAttemptHTTP2: true}
	t2 := configureHTTP2(tr)
	if t2 == nil {
		t.Fatal("configureHTTP2 returned nil, want *http2.Transport")
	}
	if t2.ReadIdleTimeout != 15*time.Second {
		t.Errorf("ReadIdleTimeout = %s, want 15s (detects silently dropped idle h2 connections)", t2.ReadIdleTimeout)
	}
	if t2.PingTimeout != 15*time.Second {
		t.Errorf("PingTimeout = %s, want 15s", t2.PingTimeout)
	}
}

// TestGetClientIdleConnectionReuse 验证连接池复用效果:
// 第一波并发用掉的空闲连接,第二波必须完全复用。
// 优化前 MaxIdleConnsPerHost 默认为 2,超过 2 的空闲连接会被丢弃,
// 第二波需要重新建连(表现为 TLS 握手开销和更长的首包延迟)。
func TestGetClientIdleConnectionReuse(t *testing.T) {
	var newConns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(30 * time.Millisecond) // 保持连接占用,保证 4 个请求并发建连
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	burst := func() {
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := GetClient("").Get(srv.URL)
				if err != nil {
					t.Errorf("request failed: %v", err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
		}
		wg.Wait()
	}

	burst()
	first := newConns.Load()
	if first != 4 {
		t.Fatalf("burst A opened %d connections, want 4", first)
	}

	time.Sleep(50 * time.Millisecond) // 等待连接进入空闲池
	burst()
	total := newConns.Load()

	if total != first {
		t.Errorf("burst B opened %d new connections (total %d), want full reuse (total %d)",
			total-first, total, first)
	}
}
