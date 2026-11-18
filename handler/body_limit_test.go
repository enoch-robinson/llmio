package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/atopos31/llmio/consts"
	"github.com/atopos31/llmio/service"
	"github.com/gin-gonic/gin"
)

// 聊天入口的请求体上限是内存闸门：请求体会被完整物化，内存随并发线性放大。
// 超限时直接 413，不再往下走。不触库，无需初始化数据库。
func TestChatHandlerRejectsOversizedBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader(make([]byte, maxChatBodyBytes+1)))
	chatHandler(c, service.BeforerOpenAI, service.ProcesserOpenAI, consts.StyleOpenAI)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestEventLoggingLimits(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/event_logging", EventLogging)

	t.Run("正常批次回计数", func(t *testing.T) {
		w := postEvents(t, router, eventLoggingBody(2))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		got := decodeCounts(t, w)
		if got.Accepted != 2 || got.Rejected != 0 {
			t.Fatalf("accepted=%d rejected=%d, want 2/0", got.Accepted, got.Rejected)
		}
	})

	t.Run("超上限截断并回 rejected", func(t *testing.T) {
		w := postEvents(t, router, eventLoggingBody(maxEventLoggingEvents+5))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		got := decodeCounts(t, w)
		if got.Accepted != maxEventLoggingEvents || got.Rejected != 5 {
			t.Fatalf("accepted=%d rejected=%d, want %d/5", got.Accepted, got.Rejected, maxEventLoggingEvents)
		}
	})

	// 未鉴权端点 + 无上限解码是外部可直接触发的内存放大点，body 闸门必须生效。
	// body 必须是合法 JSON：解码器要完整吃掉它才撞得上字节闸门，非法 JSON
	// 会先吃到 SyntaxError 走 BadRequest 分支。
	t.Run("超大 body 返回 413", func(t *testing.T) {
		pad := bytes.Repeat([]byte("a"), maxEventLoggingBodyBytes+4096)
		body := append(append([]byte(`{"pad":"`), pad...), '"', '}')
		w := postEvents(t, router, body)
		if w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusRequestEntityTooLarge)
		}
	})
}

type eventCounts struct {
	Accepted int `json:"accepted_count"`
	Rejected int `json:"rejected_count"`
}

func postEvents(t *testing.T, router *gin.Engine, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/event_logging", bytes.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func decodeCounts(t *testing.T, w *httptest.ResponseRecorder) eventCounts {
	t.Helper()
	var got eventCounts
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return got
}

func eventLoggingBody(n int) []byte {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(EventLoggingRequest{Events: make([]EventLoggingEvent, n)}); err != nil {
		panic(err)
	}
	return buf.Bytes()
}
