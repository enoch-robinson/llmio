package service

import (
	"testing"
	"time"

	"github.com/atopos31/llmio/models"
)

// benchPeakTerms 覆盖三种典型时段：限定工作日、限定星期几、跨零点不限。
func benchPeakTerms() models.PeakTerms {
	w := new(true)
	return models.PeakTerms{
		Enabled: true,
		Periods: []models.PeakPeriod{
			{Name: "peak", Start: "08:00", End: "12:00", Multiplier: 1.5, Workday: w},
			{Name: "flat", Start: "12:00", End: "18:00", Multiplier: 1.0, Days: []int{1, 2, 3, 4, 5}},
			{Name: "valley", Start: "22:00", End: "06:00", Multiplier: 0.8},
		},
	}
}

func benchPeakCalendar() models.PeakCalendar {
	return models.PeakCalendar{
		Timezone:      "Asia/Shanghai",
		Weekdays:      []int{1, 2, 3, 4, 5},
		DateOverrides: map[string]string{"2026-05-01": models.DateOverrideRest},
	}
}

// BenchmarkResolvePricingPerRequest 是热路径形态：terms 每请求从关联行新鲜
// 加载，resolver 构造一次判定一次。
func BenchmarkResolvePricingPerRequest(b *testing.B) {
	terms := benchPeakTerms()
	cal := benchPeakCalendar()
	at := time.Date(2026, 5, 11, 10, 30, 0, 0, time.UTC) // 周一上午，命中 peak
	b.ReportAllocs()
	for b.Loop() {
		NewPeakResolver(terms, cal).ResolveMultiplier(at)
	}
}

// BenchmarkPreviewSchedule30d 是时间轴预览形态：构造一次，逐分钟采样 30 天。
func BenchmarkPreviewSchedule30d(b *testing.B) {
	r := NewPeakResolver(benchPeakTerms(), benchPeakCalendar())
	start := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	for b.Loop() {
		PreviewSchedule(r, start, 30)
	}
}
