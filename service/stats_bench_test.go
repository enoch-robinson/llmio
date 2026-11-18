package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/atopos31/llmio/consts"
	"github.com/atopos31/llmio/models"
)

// benchmarkRows 与真机 30 天窗口同一量级（实测约 9 万行，取 ~5 万折中）。
const benchmarkRows = 50_000

// buildBenchLogs 构造覆盖聚合全分支的合成日志（状态混合、多维度基数、大并列组）。
func buildBenchLogs(n int) []models.ChatLog {
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	logs := make([]models.ChatLog, 0, n)
	for i := 0; i < n; i++ {
		status := consts.StatusSuccess
		switch i % 20 {
		case 3, 7:
			status = consts.StatusError
		case 11:
			status = consts.StatusRunning
		}
		l := logAt(uint(i+1), base.Add(time.Duration(i)*time.Second), status)
		l.ProviderName = fmt.Sprintf("prov-%d", i%5)
		l.ProviderModel = fmt.Sprintf("model-%d", i%25)
		l.Name = fmt.Sprintf("alias-%d", i%10)
		l.UserAgent = fmt.Sprintf("ua-%d", i%4)
		l.AuthKeyID = uint(i % 8) // 含 0 覆盖 admin 标签
		l.Error = "status: 500, body: upstream exploded"
		l.Retry = i % 3
		l.ProxyTime = time.Duration(i%900) * time.Millisecond
		l.FirstChunkTime = time.Duration(i%1500) * time.Millisecond
		l.Tps = float64(i % 97)
		l.PromptTokens = int64(200 + i%1000)
		l.CompletionTokens = int64(100 + i%500)
		l.TotalTokens = l.PromptTokens + l.CompletionTokens
		l.PromptTokensDetails.CachedTokens = int64(i % 100)
		l.InputPrice = 2.5
		l.CacheReadPrice = 1.25
		l.OutputPrice = 10
		l.Currency = "CNY"
		logs = append(logs, l)
	}
	return logs
}

// BenchmarkGroupP95 50k 条成功样本挤进同一分组（最坏基数）。
func BenchmarkGroupP95(b *testing.B) {
	const n = 50_000
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	logs := make([]models.ChatLog, 0, n)
	for i := 0; i < n; i++ {
		l := logAt(uint(i+1), base.Add(time.Duration(i)*time.Second), consts.StatusSuccess)
		l.FirstChunkTime = time.Duration(i%3000) * time.Millisecond
		logs = append(logs, l)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := newGroupAcc()
		for _, l := range logs {
			a.add("hot-group", l, 0)
		}
		_ = a.result()
	}
}

// BenchmarkAggregateWithBucket 排行榜有界化前后的内存对照基准。
func BenchmarkAggregateWithBucket(b *testing.B) {
	logs := buildBenchLogs(benchmarkRows)
	bucket := time.Hour
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := AggregateWithBucket(logs, StatsFilter{}, bucket)
		if res.KPI.Total != benchmarkRows {
			b.Fatalf("聚合行数不符：%d != %d", res.KPI.Total, benchmarkRows)
		}
		if len(res.TopTps) != leaderboardLimit || len(res.Slowest) != leaderboardLimit {
			b.Fatalf("榜单行数不符：topTps=%d slowest=%d", len(res.TopTps), len(res.Slowest))
		}
	}
}
