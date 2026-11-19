package main

import (
	"log/slog"
	"testing"
)

func TestLogLevelFromEnv(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  slog.Level
	}{
		{name: "未设置为空串回 info", value: "", want: slog.LevelInfo},
		{name: "debug", value: "debug", want: slog.LevelDebug},
		{name: "大小写与首尾空白不敏感", value: "  DEBUG ", want: slog.LevelDebug},
		{name: "info", value: "info", want: slog.LevelInfo},
		{name: "warning 别名归 warn", value: "warning", want: slog.LevelWarn},
		{name: "warn", value: "warn", want: slog.LevelWarn},
		{name: "error", value: "error", want: slog.LevelError},
		{name: "认不出回 info", value: "verbose", want: slog.LevelInfo},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := logLevelFromEnv(tc.value); got != tc.want {
				t.Fatalf("LOG_LEVEL=%q 解析成 %v，想要 %v", tc.value, got, tc.want)
			}
		})
	}
}
