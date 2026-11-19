package service

import (
	"bytes"
	"context"
	"testing"

	"github.com/atopos31/llmio/models"
	"gorm.io/gorm"
)

// 钉住 createChatIO 的形态契约：
//   - 迁移关着（默认）→ 写即压，与压缩迁移上线前的行为一致；
//   - 迁移开着 → 热路径落明文（压缩离开客户端首包路径），
//     并由下一轮迁移压好、逐字节还原。
func TestCreateChatIOPacksOrDefersWithMigrationState(t *testing.T) {
	ctx := context.Background()
	db := setupLogCompressTestDB(t)

	// 重复文本足够压得下去——EncodeVerified 要压完比原文小才成帧。
	raw := bytes.Repeat([]byte("hot path body "), 1024)

	storedType := func(id uint) string {
		t.Helper()
		var typ string
		if err := db.WithContext(ctx).
			Raw(`SELECT typeof(input) FROM chat_ios WHERE id = ?`, id).Row().Scan(&typ); err != nil {
			t.Fatalf("typeof input: %v", err)
		}
		return typ
	}
	create := func() uint {
		t.Helper()
		if err := createChatIO(ctx, 1, raw); err != nil {
			t.Fatalf("createChatIO: %v", err)
		}
		var id uint
		if err := db.WithContext(ctx).
			Raw(`SELECT COALESCE(MAX(id), 0) FROM chat_ios`).Row().Scan(&id); err != nil {
			t.Fatalf("max id: %v", err)
		}
		return id
	}

	if deferPackHotPath.Load() {
		t.Fatal("前置：热路径明文开关应为 false（迁移默认关闭）")
	}
	packed := create()
	if got := storedType(packed); got != "blob" {
		t.Fatalf("默认形态 typeof(input) = %s, want blob", got)
	}

	// 冷静期置 0：刚写的行也要进候选，默认 60s 会把它排除。
	if err := SaveLogCompressPolicy(ctx, &models.LogCompressPolicy{Enabled: true, QuiesceSec: 0}); err != nil {
		t.Fatalf("save policy: %v", err)
	}
	if !deferPackHotPath.Load() {
		t.Fatal("SaveLogCompressPolicy(Enabled:true) 应立即使热路径开关生效")
	}
	deferred := create()
	if got := storedType(deferred); got != "text" {
		t.Fatalf("迁移在场时 typeof(input) = %s, want text", got)
	}

	state, err := RunLogCompress(ctx, false)
	if err != nil {
		t.Fatalf("run pack: %v", err)
	}
	if got := storedType(deferred); got != "blob" {
		t.Fatalf("迁移后 typeof(input) = %s, want blob（scanned=%d packed=%d）", got, state.Scanned, state.Packed)
	}
	rec, err := gorm.G[models.ChatIO](db).Where("id = ?", deferred).First(ctx)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(rec.Input, raw) {
		t.Fatalf("迁移后还原的 input 与原文不一致：got %d bytes, want %d", len(rec.Input), len(raw))
	}
}
