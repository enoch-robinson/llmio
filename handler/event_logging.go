package handler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/atopos31/llmio/common"
	"github.com/gin-gonic/gin"
)

// event_logging 注册在鉴权之外，两个上限防外部直接触发的解码放大：超大 body
// 或超长数组白白解码一轮再整包丢弃。
const (
	maxEventLoggingBodyBytes = 1 << 20
	maxEventLoggingEvents    = 1000
)

type EventLoggingRequest struct {
	Events []EventLoggingEvent `json:"events"`
}

type EventLoggingEvent struct {
	EventType string                `json:"event_type"`
	EventData EventLoggingEventData `json:"event_data"`
}

type EventLoggingEventData struct {
	EventName          string          `json:"event_name"`
	ClientTimestamp    string          `json:"client_timestamp"`
	Model              string          `json:"model"`
	SessionID          string          `json:"session_id"`
	UserType           string          `json:"user_type"`
	Betas              string          `json:"betas"`
	Env                EventLoggingEnv `json:"env"`
	Entrypoint         string          `json:"entrypoint"`
	IsInteractive      bool            `json:"is_interactive"`
	ClientType         string          `json:"client_type"`
	AdditionalMetadata string          `json:"additional_metadata"`
	DeviceID           string          `json:"device_id"`
}

type EventLoggingEnv struct {
	Platform              string `json:"platform"`
	NodeVersion           string `json:"node_version"`
	Terminal              string `json:"terminal"`
	PackageManagers       string `json:"package_managers"`
	Runtimes              string `json:"runtimes"`
	IsRunningWithBun      bool   `json:"is_running_with_bun"`
	IsCI                  bool   `json:"is_ci"`
	IsClaubbit            bool   `json:"is_claubbit"`
	IsGithubAction        bool   `json:"is_github_action"`
	IsClaudeCodeAction    bool   `json:"is_claude_code_action"`
	IsClaudeAIAuth        bool   `json:"is_claude_ai_auth"`
	Version               string `json:"version"`
	Arch                  string `json:"arch"`
	IsClaudeCodeRemote    bool   `json:"is_claude_code_remote"`
	DeploymentEnvironment string `json:"deployment_environment"`
}

func EventLogging(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxEventLoggingBodyBytes)
	var req EventLoggingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			common.ErrorWithHttpStatus(c, http.StatusRequestEntityTooLarge, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body too large (max %d bytes)", maxEventLoggingBodyBytes))
			return
		}
		common.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	rejected := 0
	if len(req.Events) > maxEventLoggingEvents {
		rejected = len(req.Events) - maxEventLoggingEvents
		req.Events = req.Events[:maxEventLoggingEvents]
	}
	c.JSON(http.StatusOK, gin.H{
		"accepted_count": len(req.Events),
		"rejected_count": rejected,
	})
}
