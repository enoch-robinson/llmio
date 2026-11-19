package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/atopos31/llmio/models"
)

// WAL 下"库多大"与"备份够不够新"都要多算一个 `-wal`。
// 这两条口径错了不会报错，只会让界面说假话：前者少报占用，后者把半截备份
// 判成可用——而迁移是不可逆的。
func TestDBFileSizeAndBackupCountWAL(t *testing.T) {
	t.Setenv(models.WALEnv, "true")
	path := filepath.Join(t.TempDir(), "wal-size.db")
	models.Init(context.Background(), path)
	t.Cleanup(func() {
		if sqlDB, err := models.DB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	if !models.WALEnabled() {
		t.Fatalf("开关打开后库没进 WAL：journal_mode=%q", models.JournalMode)
	}

	// 写够体积，让 -wal 里确实有还没 checkpoint 的页。
	if err := models.DB.Exec(`CREATE TABLE wal_probe (id INTEGER PRIMARY KEY, v TEXT)`).Error; err != nil {
		t.Fatalf("建表失败：%v", err)
	}
	body := make([]byte, 4096)
	for i := range body {
		body[i] = 'x'
	}
	for i := 0; i < 32; i++ {
		if err := models.DB.Exec(`INSERT INTO wal_probe (v) VALUES (?)`, string(body)).Error; err != nil {
			t.Fatalf("写入失败：%v", err)
		}
	}

	mainInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("量主库文件失败：%v", err)
	}
	walInfo, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatalf("WAL 下应当有 -wal 文件：%v", err)
	}
	if walInfo.Size() == 0 {
		t.Fatal("-wal 是空的，这条用例证明不了任何事（数据可能已被 checkpoint）")
	}

	size, err := DBFileSize()
	if err != nil {
		t.Fatalf("DBFileSize 失败：%v", err)
	}
	if want := mainInfo.Size() + walInfo.Size(); size != want {
		t.Fatalf("库占用应当含 -wal：got %d want %d（主库 %d + wal %d）",
			size, want, mainInfo.Size(), walInfo.Size())
	}
	if size <= mainInfo.Size() {
		t.Fatal("只量了主库：-wal 里那部分被漏掉了")
	}

	// 半截备份（只拷了主库、丢了未 checkpoint 的页）必须判 stale。
	bak := filepath.Join(t.TempDir(), "llmio.db.bak")
	if err := os.WriteFile(bak, make([]byte, mainInfo.Size()), 0o644); err != nil {
		t.Fatalf("造备份失败：%v", err)
	}
	if rec := VerifyBackup(bak, size); rec.Source != "stale" {
		t.Fatalf("只拷主库的备份应当判 stale，实得 %q", rec.Source)
	}

	// checkpoint 之后 -wal 被截到 0，此时同样大小的备份就是完整的。
	checkpointWAL(t)
	after, err := DBFileSize()
	if err != nil {
		t.Fatalf("checkpoint 后 DBFileSize 失败：%v", err)
	}
	if err := os.WriteFile(bak, make([]byte, after), 0o644); err != nil {
		t.Fatalf("重造备份失败：%v", err)
	}
	if rec := VerifyBackup(bak, after); rec.Source != "manual" {
		t.Fatalf("checkpoint 后同尺寸的备份应当可用，实得 %q", rec.Source)
	}
}

// checkpointWAL 把 -wal 全部落进主库并截断。TRUNCATE 是我们希望的形态：
// 文件回到 0 字节，"主库大小 = 库大小"这条旧口径重新成立。
func checkpointWAL(t *testing.T) {
	t.Helper()
	var mode, frames, checkpointed int
	if err := models.DB.Raw(`PRAGMA wal_checkpoint(TRUNCATE)`).
		Row().Scan(&mode, &frames, &checkpointed); err != nil {
		t.Fatalf("wal_checkpoint 失败：%v", err)
	}
	if mode != 0 {
		t.Fatalf("checkpoint 被挡住了（还有读者持着快照？）：busy=%d", mode)
	}
}
