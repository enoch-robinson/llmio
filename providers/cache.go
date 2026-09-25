package providers

import (
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

type clientCache struct {
	mu      sync.RWMutex
	clients map[string]*http.Client
}

var cache = &clientCache{
	clients: make(map[string]*http.Client),
}

var dialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

// GetClient returns an http.Client for the given proxy, cached per proxy URL.
// 同一 proxy 共享一个 Transport 及其连接池;响应头阶段超时由调用方按请求控制
// (见 service.BalanceChat),不再作为 Transport 配置,避免按超时值碎片化连接池。
func GetClient(proxyURL string) *http.Client {
	cache.mu.RLock()
	if client, exists := cache.clients[proxyURL]; exists {
		cache.mu.RUnlock()
		return client
	}
	cache.mu.RUnlock()

	cache.mu.Lock()
	defer cache.mu.Unlock()

	// Double-check after acquiring write lock
	if client, exists := cache.clients[proxyURL]; exists {
		return client
	}

	proxyFunc := http.ProxyFromEnvironment
	if proxyURL != "" {
		if u, err := url.Parse(proxyURL); err == nil {
			proxyFunc = http.ProxyURL(u)
		}
	}

	transport := &http.Transport{
		Proxy:                 proxyFunc,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// HTTP/2 连接 PING 探活:复用连接前先确认对端存活,
	// 及时剔除被 LB/中间设备静默丢弃的空闲连接,
	// 避免 "http2: timeout awaiting response headers" 一类间歇性首包超时。
	configureHTTP2(transport)

	client := &http.Client{Transport: transport}

	cache.clients[proxyURL] = client
	return client
}

// configureHTTP2 启用 HTTP/2 并配置 PING 探活,
// 返回 *http2.Transport 以便测试断言探活参数。
func configureHTTP2(transport *http.Transport) *http2.Transport {
	t2, err := http2.ConfigureTransports(transport)
	if err != nil {
		return nil
	}
	t2.ReadIdleTimeout = 15 * time.Second
	t2.PingTimeout = 15 * time.Second
	return t2
}
