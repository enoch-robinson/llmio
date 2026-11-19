package models

import (
	"context"
	"path/filepath"
	"testing"

	"gorm.io/gorm"
)

// PrepareBatch 与 CommitBatch 之间，别的写者插入了同一批块（迁移在事务外准备、
// 生产写路径并发插入是真实形状）：提交阶段必须把引用落到已存在的块上——
// 不报错、不写重复块、不留无块的组行。
func TestBlockStore_CommitBatchToleratesConcurrentInsert(t *testing.T) {
	db := openCompressDB(t)
	ctx := context.Background()
	s := NewBlockStore(0)

	body := distinctRow(31, 96<<10)

	// 事务外的准备（迁移路径的形状）：此刻库里没有这些块。
	plan, err := s.PrepareBatch(ctx, db, [][]byte{body})
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}

	// 模拟并发写者在准备之后、提交之前插入了同一批块。
	concurrent := NewBlockStore(0)
	wantRefs, err := concurrent.PutBatch(ctx, db, [][]byte{body})
	if err != nil {
		t.Fatalf("并发写入: %v", err)
	}

	var groupsBefore, blocksBefore, groupsAfter, blocksAfter int64
	db.Model(&BlockGroup{}).Count(&groupsBefore)
	db.Model(&Block{}).Count(&blocksBefore)

	var refs [][]uint32
	if err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		refs, err = s.CommitBatch(ctx, tx, plan)
		return err
	}); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}

	db.Model(&BlockGroup{}).Count(&groupsAfter)
	db.Model(&Block{}).Count(&blocksAfter)
	if groupsAfter != groupsBefore || blocksAfter != blocksBefore {
		t.Fatalf("整组已存在时不该写组/块：组 %d→%d，块 %d→%d",
			groupsBefore, groupsAfter, blocksBefore, blocksAfter)
	}

	if len(refs) != 1 || len(wantRefs) != 1 || len(refs[0]) != len(wantRefs[0]) {
		t.Fatalf("引用序列 = %v，want %v", refs, wantRefs)
	}
	for i := range refs[0] {
		if refs[0][i] != wantRefs[0][i] {
			t.Fatalf("第 %d 个引用 = %d，want %d（应落到已存在的块）", i, refs[0][i], wantRefs[0][i])
		}
	}

	got, err := s.Get(ctx, db, refs[0])
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(body) {
		t.Fatal("按提交得到的引用还原，内容与原文不一致")
	}
}

// BenchmarkPrepareBatch 量迁移路径在事务外做掉的 CPU：分块、SHA-256、
// level-9 组帧与解压自检（这些在 H4 之前全在写事务里）。
//
// 8 行 × 512 KiB，约默认一批（32 MiB）的 1/8，CPU 与字节数成正比。
// distinctRow 是不可压数据，每组都会真的走一遍 level-9 压缩与自检，是上界。
func BenchmarkPrepareBatch(b *testing.B) {
	path := filepath.Join(b.TempDir(), "prepare-bench.db")
	Init(context.Background(), path)
	b.Cleanup(func() {
		if sqlDB, err := DB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	const (
		rows  = 8
		each  = 512 << 10
		total = rows * each
	)
	plains := make([][]byte, rows)
	for i := range plains {
		plains[i] = distinctRow(100+i, each)
	}

	b.SetBytes(int64(total))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := blockStore.PrepareBatch(context.Background(), DB, plains); err != nil {
			b.Fatalf("PrepareBatch: %v", err)
		}
	}
}
