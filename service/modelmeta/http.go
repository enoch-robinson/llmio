package modelmeta

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/atopos31/llmio/providers"
)

// fetchTimeout 是一次抓取的响应头超时。
//
// 5.3 MB（gzip 后约 531 KB）在慢线路上也可能要十几秒，30 秒是照着
// 「最慢的正常情况 + 余量」定的，不是照着快照定的。
const fetchTimeout = 30 * time.Second

// errFetchHeaderTimeout 表示在预算内未等到响应头，与上游断开（context.Canceled）
// 区分开，避免后台刷新把网络抖动误判成别的原因。
var errFetchHeaderTimeout = errors.New("timeout waiting for response headers")

// httpDoer 让两个 Source 的抓取可以被测试换掉。
type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// timeoutDoer 在 httpDoer 外补回按请求的响应头超时。providers.GetClient 不再
// 把超时配进 Transport（避免按超时值碎片化连接池），头阶段限时改由这里以
// AfterFunc + WithCancelCause 实现：收到响应头即停止计时，不限制 body 传输；
// cancel 挂在包装的响应体上，读完或关闭时释放。后台刷新的 ctx 来自
// context.WithoutCancel(Background)，不补这一层 cancel 就会随抓取泄漏。
type timeoutDoer struct {
	doer  httpDoer
	limit time.Duration
}

func (d timeoutDoer) Do(req *http.Request) (*http.Response, error) {
	attemptCtx, cancel := context.WithCancelCause(req.Context())
	req = req.WithContext(attemptCtx)

	timer := time.AfterFunc(d.limit, func() { cancel(errFetchHeaderTimeout) })
	res, err := d.doer.Do(req)
	if err != nil {
		timer.Stop()
		if context.Cause(attemptCtx) == errFetchHeaderTimeout {
			err = errFetchHeaderTimeout
		}
		cancel(nil)
		return nil, err
	}
	timer.Stop()
	res.Body = &cancelBody{ReadCloser: res.Body, cancel: cancel}
	return res, nil
}

// cancelBody 在响应体读完或关闭时释放按请求包装的 attempt context。
// cancel 幂等，EOF / 读错误 / Close 多次触发都安全。
type cancelBody struct {
	io.ReadCloser
	cancel context.CancelCauseFunc
}

func (b *cancelBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.cancel(nil)
	}
	return n, err
}

func (b *cancelBody) Close() error {
	b.cancel(nil)
	return b.ReadCloser.Close()
}

// defaultHTTPClient 复用 providers 的客户端缓存，从而继承同一份代理配置。
//
// proxy 传空串走 http.ProxyFromEnvironment —— 这两个端点没有"属于哪个上游"
// 可言，环境变量里的代理是唯一合理的来源。
//
// 关于 gzip：**不显式设 Accept-Encoding**。Go 的 Transport 会在请求没有
// 该头时自己加上 gzip 并透明解压，效果与手写一遍相同（5,315,044 B → 530,868 B），
// 但少一处"设了头就得自己解压"的坑。
func defaultHTTPClient() httpDoer {
	return timeoutDoer{doer: providers.GetClient(""), limit: fetchTimeout}
}
