package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/atopos31/llmio/balancers"
	"github.com/atopos31/llmio/consts"
	"github.com/atopos31/llmio/models"
	"github.com/atopos31/llmio/pkg/token"
	"github.com/atopos31/llmio/providers"
	"github.com/samber/lo"
	"github.com/tidwall/sjson"
	"gorm.io/gorm"
)

// errResponseHeaderTimeout 表示在预算时间内未收到上游响应头,
// 用于区分主动的头阶段超时与客户端断开(context.Canceled)。
var errResponseHeaderTimeout = errors.New("timeout waiting for response headers")

// doWithHeaderTimeout 包装 client.Do,仅在响应头阶段施加超时:
// 计时器到点前收到响应头则停止计时,后续 body 传输不受影响。
// headerTimeout<=0 表示不限制头等待时间(与原 ResponseHeaderTimeout=0 语义一致)。
// 成功时返回的 cancel 与响应体生命周期一致,调用方在响应体消费完毕后必须调用。
func doWithHeaderTimeout(ctx context.Context, client *http.Client, req *http.Request, headerTimeout time.Duration) (*http.Response, context.CancelCauseFunc, error) {
	attemptCtx, cancelAttempt := context.WithCancelCause(ctx)
	req = req.WithContext(attemptCtx)

	var headerTimer *time.Timer
	if headerTimeout > 0 {
		headerTimer = time.AfterFunc(headerTimeout, func() {
			cancelAttempt(errResponseHeaderTimeout)
		})
	}

	res, err := client.Do(req)
	if err != nil {
		if headerTimer != nil {
			headerTimer.Stop()
			if context.Cause(attemptCtx) == errResponseHeaderTimeout {
				err = errResponseHeaderTimeout
			}
		}
		cancelAttempt(nil)
		return nil, nil, err
	}
	if headerTimer != nil {
		headerTimer.Stop()
	}
	// 成功路径不 cancelAttempt:cancel 会中断后续 body 读取。
	return res, cancelAttempt, nil
}

// BalanceChat 按负载均衡策略转发请求。
// 返回的 cancel 与响应体生命周期一致,调用方在响应体消费完毕后必须调用,
// 以释放按请求包装的上游 context。
func BalanceChat(ctx context.Context, start time.Time, style string, before Before, providersWithMeta ProvidersWithMeta, reqMeta models.ReqMeta) (*http.Response, *models.ChatLog, context.CancelCauseFunc, error) {
	slog.Info("request", "model", before.Model, "stream", before.Stream, "tool_call", before.toolCall, "structured_output", before.structuredOutput, "image", before.image)

	providerMap := providersWithMeta.ProviderMap

	// 收集重试过程中的err日志
	retryLog := make(chan models.ChatLog, providersWithMeta.MaxRetry)
	defer close(retryLog)

	go RecordRetryLog(context.Background(), retryLog)

	// 选择负载均衡策略
	var balancer balancers.Balancer
	switch providersWithMeta.Strategy {
	case consts.BalancerLottery:
		balancer = balancers.NewLottery(providersWithMeta.WeightItems)
	case consts.BalancerRotor:
		balancer = balancers.NewRotor(providersWithMeta.WeightItems)
	default:
		balancer = balancers.NewLottery(providersWithMeta.WeightItems)
	}

	// 是否开启熔断
	if providersWithMeta.Breaker {
		balancer = balancers.BalancerWrapperBreaker(balancer)
	}

	// 响应头阶段超时:仅约束等待响应头的时间,收到响应头即停止计时,
	// 不限制流式 body 的传输时长;首字延迟高的推理模型也能在预算内建立流。
	responseHeaderTimeout := time.Second * time.Duration(providersWithMeta.TimeOut)

	authKeyID, _ := ctx.Value(consts.ContextKeyAuthKeyID).(uint)
	authKeyIOLog, _ := ctx.Value(consts.ContextKeyAuthKeyIOLog).(bool)

	traceID, err := token.GenerateRandomChars(10)
	if err != nil {
		return nil, nil, nil, err
	}

	timer := time.NewTimer(time.Second * time.Duration(providersWithMeta.TimeOut))
	defer timer.Stop()
	for retry := range providersWithMeta.MaxRetry {
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		case <-timer.C:
			return nil, nil, nil, errors.New("retry time out")
		default:
			// 加权负载均衡
			id, err := balancer.Pop()
			if err != nil {
				return nil, nil, nil, fmt.Errorf("balancer pop err: %v, traceID: %s", err, traceID)
			}

			modelWithProvider, ok := providersWithMeta.ModelWithProviderMap[id]
			if !ok {
				// 数据不一致，移除该模型避免下次重复命中
				balancer.Delete(id)
				continue
			}

			provider := providerMap[modelWithProvider.ProviderID]

			chatModel, err := providers.New(provider.Type, provider.Config, provider.Proxy)
			if err != nil {
				return nil, nil, nil, err
			}

			client := providers.GetClient(provider.Proxy)

			slog.Info("using provider", "provider", provider.Name, "model", modelWithProvider.ProviderModel)

			log := models.ChatLog{
				Name:           before.Model,
				TraceID:        traceID,
				ProviderModel:  modelWithProvider.ProviderModel,
				ProviderName:   provider.Name,
				Status:         consts.StatusRunning,
				Style:          style,
				UserAgent:      reqMeta.UserAgent,
				RemoteIP:       reqMeta.RemoteIP,
				AuthKeyID:      authKeyID,
				SessionID:      before.SessionID,
				ChatIO:         authKeyIOLog,
				Retry:          retry,
				ProxyTime:      time.Since(start),
				InputPrice:     lo.FromPtrOr(modelWithProvider.InputPrice, 0),
				CacheReadPrice: lo.FromPtrOr(modelWithProvider.CacheReadPrice, 0),
				OutputPrice:    lo.FromPtrOr(modelWithProvider.OutputPrice, 0),
				Currency:       modelWithProvider.Currency,
			}
			// 根据请求原始请求头 是否透传请求头 自定义请求头 构建新的请求头
			withHeader := lo.FromPtrOr(modelWithProvider.WithHeader, false)
			headers := BuildHeaders(reqMeta.Header, withHeader, modelWithProvider.CustomerHeaders, before.Stream)

			rawBody, err := buildUpstreamBody(before.raw, modelWithProvider.ExtraBody)
			if err != nil {
				retryLog <- log.WithError(err)
				balancer.Delete(id)
				continue
			}

			req, err := chatModel.BuildReq(ctx, headers, modelWithProvider.ProviderModel, rawBody)
			if err != nil {
				retryLog <- log.WithError(err)
				// 构建请求失败 移除待选
				balancer.Delete(id)
				continue
			}

			// 仅响应头阶段的超时:收到响应头后停止计时,后续 body 传输不受影响。
			res, cancelAttempt, err := doWithHeaderTimeout(ctx, client, req, responseHeaderTimeout)
			if err != nil {
				retryLog <- log.WithError(err)
				// 请求失败 移除待选
				balancer.Delete(id)
				continue
			}
			// 此处不 cancelAttempt:cancel 会中断后续 body 读取,
			// 由调用方在响应体消费完毕后调用。

			if res.StatusCode != http.StatusOK {
				byteBody, err := io.ReadAll(res.Body)
				if err != nil {
					slog.Error("read body error", "error", err)
				}
				retryLog <- log.WithError(fmt.Errorf("status: %d, body: %s", res.StatusCode, string(byteBody)))

				if res.StatusCode == http.StatusTooManyRequests {
					// 达到RPM限制 降低权重
					balancer.Reduce(id)
				} else {
					// 非RPM限制 移除待选
					balancer.Delete(id)
				}
				res.Body.Close()
				cancelAttempt(nil)
				continue
			}

			if provider.ErrorMatcher != "" {
				contentType := strings.ToLower(res.Header.Get("Content-Type"))
				// 流式正常返回通常是 text/event-stream，不提前消费响应体避免影响转发。
				if !strings.Contains(contentType, "text/event-stream") {
					byteBody, err := io.ReadAll(res.Body)
					if err != nil {
						retryLog <- log.WithError(fmt.Errorf("read body failed: %w", err))
						balancer.Delete(id)
						res.Body.Close()
						cancelAttempt(nil)
						continue
					}

					if matched, sample := matchProviderBodyError(string(byteBody), provider.ErrorMatcher); matched {
						retryLog <- log.WithError(fmt.Errorf("response matched provider error sample %q, body: %s", sample, string(byteBody)))
						balancer.Delete(id)
						res.Body.Close()
						cancelAttempt(nil)
						continue
					}

					res.Body = io.NopCloser(bytes.NewReader(byteBody))
				}
			}

			balancer.Success(id)

			return res, &log, cancelAttempt, nil
		}
	}

	return nil, nil, nil, fmt.Errorf("All retry failed, trace ID: %s", traceID)
}

func buildUpstreamBody(raw []byte, extraBody map[string]any) ([]byte, error) {
	rawBody := raw
	if len(extraBody) > 0 {
		for key, value := range extraBody {
			var err error
			rawBody, err = sjson.SetBytes(rawBody, key, value)
			if err != nil {
				slog.Warn("failed to set extra body key", "key", key, "error", err)
			}
		}
	}

	rawBody, err := sjson.DeleteBytes(rawBody, "session_id")
	if err != nil {
		return nil, fmt.Errorf("delete session_id from upstream body: %w", err)
	}
	return rawBody, nil
}

func RecordRetryLog(ctx context.Context, retryLog chan models.ChatLog) {
	for log := range retryLog {
		if _, err := SaveChatLog(ctx, log); err != nil {
			slog.Error("save chat log error", "error", err)
		}
	}
}

func RecordLog(ctx context.Context, reqStart time.Time, reader io.ReadCloser, processer Processer, logId uint, before Before, ioLog bool) {
	recordFunc := func() error {
		defer reader.Close()
		if ioLog {
			if err := gorm.G[models.ChatIO](models.DB).Create(ctx, &models.ChatIO{
				Input: string(before.raw),
				LogId: logId,
			}); err != nil {
				return err
			}
		}
		log, output, err := processer(ctx, reader, before.Stream, reqStart)
		if err != nil {
			return err
		}
		log.Status = consts.StatusSuccess
		if _, err := gorm.G[models.ChatLog](models.DB).Where("id = ?", logId).Updates(ctx, *log); err != nil {
			return err
		}
		if ioLog {
			if _, err := gorm.G[models.ChatIO](models.DB).Where("log_id = ?", logId).Updates(ctx, models.ChatIO{OutputUnion: *output}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := recordFunc(); err != nil {
		if _, err := gorm.G[models.ChatLog](models.DB).Where("id = ?", logId).Updates(ctx, models.ChatLog{
			Status: consts.StatusError,
			Error:  err.Error(),
		}); err != nil {
			slog.Error("record log error", "error", err)
		}
	}
}

func SaveChatLog(ctx context.Context, log models.ChatLog) (uint, error) {
	if err := gorm.G[models.ChatLog](models.DB).Create(ctx, &log); err != nil {
		return 0, err
	}
	return log.ID, nil
}

func BuildHeaders(source http.Header, withHeader bool, customHeaders map[string]string, stream bool) http.Header {
	header := http.Header{}
	if withHeader {
		header = source.Clone()
	}

	if stream {
		header.Set("X-Accel-Buffering", "no")
	}

	header.Del("Authorization")
	header.Del("X-Api-Key")
	header.Del("X-Goog-Api-Key")

	for key, value := range customHeaders {
		header.Set(key, value)
	}

	return header
}

type ProvidersWithMeta struct {
	ModelWithProviderMap map[uint]models.ModelWithProvider
	WeightItems          map[uint]int
	ProviderMap          map[uint]models.Provider
	MaxRetry             int
	TimeOut              int
	Strategy             string
	Breaker              bool
}

func ProvidersWithMetaBymodelsName(ctx context.Context, style string, before Before) (*ProvidersWithMeta, error) {
	model, err := gorm.G[models.Model](models.DB).Where("name = ?", before.Model).First(ctx)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if _, err := SaveChatLog(ctx, models.ChatLog{
				Name:      before.Model,
				Status:    consts.StatusError,
				Style:     style,
				SessionID: before.SessionID,
				Error:     err.Error(),
			}); err != nil {
				return nil, err
			}
			return nil, errors.New("not found model " + before.Model)
		}
		return nil, err
	}

	modelWithProviderChain := gorm.G[models.ModelWithProvider](models.DB).Where("model_id = ?", model.ID).Where("status = ?", true)

	if before.toolCall {
		modelWithProviderChain = modelWithProviderChain.Where("tool_call = ?", true)
	}

	if before.structuredOutput {
		modelWithProviderChain = modelWithProviderChain.Where("structured_output = ?", true)
	}

	if before.image {
		modelWithProviderChain = modelWithProviderChain.Where("image = ?", true)
	}

	modelWithProviders, err := modelWithProviderChain.Find(ctx)
	if err != nil {
		return nil, err
	}

	if len(modelWithProviders) == 0 {
		return nil, errors.New("not provider for model " + before.Model)
	}

	modelWithProviderMap := lo.KeyBy(modelWithProviders, func(mp models.ModelWithProvider) uint { return mp.ID })

	providers, err := gorm.G[models.Provider](models.DB).
		Where("id IN ?", lo.Map(modelWithProviders, func(mp models.ModelWithProvider, _ int) uint { return mp.ProviderID })).
		Where("type = ?", style).
		Find(ctx)
	if err != nil {
		return nil, err
	}

	providerMap := lo.KeyBy(providers, func(p models.Provider) uint { return p.ID })

	weightItems := make(map[uint]int)
	for _, mp := range modelWithProviders {
		if _, ok := providerMap[mp.ProviderID]; !ok {
			continue
		}
		weightItems[mp.ID] = mp.Weight
	}

	return &ProvidersWithMeta{
		ModelWithProviderMap: modelWithProviderMap,
		WeightItems:          weightItems,
		ProviderMap:          providerMap,
		MaxRetry:             model.MaxRetry,
		TimeOut:              model.TimeOut,
		Strategy:             model.Strategy,
		Breaker:              lo.FromPtrOr(model.Breaker, false),
	}, nil
}
