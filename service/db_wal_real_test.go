package service

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"
)

// WAL 真机验收。docs/db-wal-evaluation.md 第七节的清单里，能在 go test 里
// 做掉的一半在这里：第七节的第 3、4、7 条与第 1/2 条的机制面，外加一条
// 给「SaveChatLog 是否异步化」决策用的真磁盘提交吞吐。第 5 条（VACUUM）
// 在 TestWAL_RealCopyVacuumOnWAL，要显式点名才跑。
//
// 必须对一份**可写副本**跑：用例会开 WAL、真写、最后把副本翻回 delete。
// 本环境没有 7 GiB 生产库副本（db/llmio.db 是 274 KB 空库），默认跳过。跑法：
//
//	$env:LLMIO_REAL_DB_COPY="D:\llmio-test\work-wal\real.db"
//	go test ./service/ -run TestWAL_RealCopyAcceptance -v -timeout 30m
//
// 第 1 条的流量面（真实流量的 BUSY 计数）、第 2 条（p95 首包）与第 6 条
// （容器文件系统形态）没法自动化，跑法见文档第七节。
func TestWAL_RealCopyAcceptance(t *testing.T) {
	srcPath := realDBForMigration(t)

	work := copyForMigrationNamed(t, srcPath, "work-wal", "wal.db")
	t.Cleanup(func() {
		_ = os.Remove(work)
		_ = os.Remove(work + "-wal")
		_ = os.Remove(work + "-shm")
	})

	srcMode := journalMode(t, work)
	t.Logf("源库 %s（%s，journal_mode=%s）", srcPath, humanBytesReal(fileSize(t, srcPath)), srcMode)

	t.Run("长读不再挡写", func(t *testing.T) { walRealReadBlocksWriter(t, work) })
	t.Run("wal峰值与checkpoint推进", func(t *testing.T) { walRealCheckpointGrowth(t, work) })
	t.Run("提交吞吐", func(t *testing.T) { walRealCommitThroughput(t, work) })
	// 回退要等前面所有连接都关干净（journal_mode=DELETE 不允许还有活跃连接），
	// 所以放最后。
	t.Run("回退到delete", func(t *testing.T) { walRealRollbackToDelete(t, work) })
}

// walRealDSN 生产同形的 WAL 连接串：三条 pragma 与生产 DSN 逐字一致，
// busy_timeout 同样是 5000ms。读连接**不带** _txlock=immediate——
// 带了会让读者去抢写锁，量不到「读不挡写」（见 models/wal_test.go 的教训）。
func walRealDSN(path string) string {
	return path + "?_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=busy_timeout(5000)"
}

// walFileSize 量 -wal 文件；不存在（没开过 WAL 或刚被截断）算 0。
func walFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// requireWALMode 断言这份库真的进了 WAL。
//
// 这一步顺带把第七节第 6 条（文件系统形态）在测试机上验了一半：WAL 依赖
// -shm 的共享内存语义，NFS/SMB 上要么开不出来（这里就会红），要么更糟。
func requireWALMode(t *testing.T, db *sql.DB) {
	t.Helper()
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("读 journal_mode 失败：%v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode=%q，没进 WAL——多半是网络文件系统（NFS/SMB）上，DB_WAL 不能开", mode)
	}
}

// walHeavyQ 就是闯过祸的那句全表载荷求和（见 db_stats_real_test.go 的说明）——
// 生产长读的形状，用它来持锁才量得到真东西。
const walHeavyQ = `SELECT COALESCE(sum(length(CAST(input AS BLOB))` +
	` + length(CAST(of_string AS BLOB))` +
	` + length(CAST(of_string_array AS BLOB))), 0) FROM chat_ios WHERE deleted_at IS NULL`

// walRealReadBlocksWriter：WAL 下长读持锁期间，写请求零失败、不等满 busy_timeout。
//
// 这是第七节第 1/2 条的机制面：真实流量里的 BUSY 计数与 p95 只能在生产侧数，
// 但「读还挡不挡写」这个机制在副本上就能验死。读用的是 db_stats_real_test 里
// 那句闯过祸的全表载荷求和——它就是生产长读的形状。
func walRealReadBlocksWriter(t *testing.T, work string) {
	writer := openSQL(t, walRealDSN(work), 1)
	defer writer.Close()
	requireWALMode(t, writer)

	reader := openSQL(t, walRealDSN(work), 1)
	defer reader.Close()

	victim := lastRowID(t, reader)
	t.Logf("副本已进 WAL，busy_timeout=5000ms，锁冲突用 chat_ios.id=%d 上的真实 UPDATE 制造", victim)

	// 热身：刚复制出来的 7 GB 全是脏页，不热身的话第一次写会把内核刷盘的账
	// 算到 WAL 头上（db_stats_real_test 同款理由）。
	warmup := runWriter(t, writer, victim, 3*time.Second, nil)
	t.Logf("【写】热身 %d 次（不计）：%s", warmup.count, warmup.stall())

	win := runWriter(t, writer, victim, 30*time.Second, func(done <-chan struct{}) {
		for i := 0; i < 3; i++ {
			if isClosed(done) {
				return
			}
			if _, err := reader.Exec(walHeavyQ); err != nil {
				t.Logf("【读】重读失败（WAL 下不该发生，但写了也不该挡住别人）：%v", err)
			}
		}
	})
	t.Logf("【写】3 次全表重读期间：%s", win.stall())

	if win.failed > 0 {
		t.Fatalf("WAL 下 %d 次写请求等满 busy_timeout 失败（最长 %s）——读挡写在真机上依然存在",
			win.failed, win.worstAll)
	}
	if win.worstAll >= sqliteBusyTimeout {
		t.Fatalf("WAL 下写请求被拖了 %s，顶到 busy_timeout——这就是要消灭的那个 500 前兆",
			win.worstAll)
	}
}

// walRealCheckpointGrowth：第七节第 3、4 条。
//
// 并发写 + 持续长读时 -wal 涨到多大（第 3 条）；PASSIVE checkpoint 在长读
// 期间是否 busy、读释放后能不能追上、TRUNCATE 能不能截干净（第 4 条）。
// 增长量本身是磁盘相关的观察值，只报不判；判的是两个硬下限：写零失败、
// 读释放后 TRUNCATE 能把 -wal 截到 0。
func walRealCheckpointGrowth(t *testing.T, work string) {
	writer := openSQL(t, walRealDSN(work), 1)
	defer writer.Close()
	requireWALMode(t, writer)
	reader := openSQL(t, walRealDSN(work), 1)
	defer reader.Close()
	ckptr := openSQL(t, walRealDSN(work), 1)
	defer ckptr.Close()

	victim := lastRowID(t, reader)

	// 持住一个读快照：开 deferred 事务后把结果集挂住不关。这正是生产里
	// 「状态页长读」的形状——读事务不结束，checkpoint 就追不上最新一笔。
	tx, err := reader.Begin()
	if err != nil {
		t.Fatalf("开读事务失败：%v", err)
	}
	rows, err := tx.Query(`SELECT id FROM chat_ios WHERE deleted_at IS NULL LIMIT 500`)
	if err != nil {
		t.Fatalf("挂住读快照失败：%v", err)
	}
	if !rows.Next() {
		t.Fatal("读不出任何行，快照没真正建立")
	}

	// 写的同时每 500ms 采样一次 -wal 大小。只有采样协程写 peak，主协程在
	// sampleDone 收到信号后才读——靠 channel 的先后关系免锁。
	walPath := work + "-wal"
	var peakWAL int64
	stopSample := make(chan struct{})
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSample:
				return
			case <-ticker.C:
				if n := walFileSize(walPath); n > peakWAL {
					peakWAL = n
				}
			}
		}
	}()

	win := runWriter(t, writer, victim, 10*time.Second, func(done <-chan struct{}) {
		// 长读还持着、写入还在流：此刻的 PASSIVE 就是第 4 条问的现场。
		busy, logPages, ckptPages := runCheckpoint(t, ckptr, "PASSIVE")
		t.Logf("【checkpoint】长读持锁 + 并发写时 PASSIVE：busy=%d，WAL 里 %d 页，其中 %d 页已并回主库",
			busy, logPages, ckptPages)
	})
	close(stopSample)
	<-sampleDone

	t.Logf("【写】期间 %s", win.stall())
	t.Logf("【-wal】并发写 + 长读 10 秒内峰值 %s", humanBytesReal(peakWAL))

	// 读释放后，checkpoint 要能追上；TRUNCATE 要能截干净。
	_ = rows.Close()
	_ = tx.Rollback()

	busy, logPages, ckptPages := runCheckpoint(t, ckptr, "PASSIVE")
	t.Logf("【checkpoint】读释放后 PASSIVE：busy=%d，WAL 里 %d 页，并回 %d 页", busy, logPages, ckptPages)
	if busy != 0 {
		t.Fatalf("读释放后 PASSIVE 仍 busy=%d——有快照没放干净，checkpoint 永远追不上", busy)
	}

	_, _, _ = runCheckpoint(t, ckptr, "TRUNCATE")
	walAfter := walFileSize(walPath)
	t.Logf("【-wal】TRUNCATE 后 %s", humanBytesReal(walAfter))
	if walAfter != 0 {
		t.Fatalf("TRUNCATE 后 -wal 还有 %d 字节——没有活跃读时它应该归零", walAfter)
	}

	if win.failed > 0 {
		t.Fatalf("并发写期间 %d 次失败（最长 %s）", win.failed, win.worstAll)
	}
}

// runCheckpoint 跑一次 wal_checkpoint(mode)，返回 (busy, WAL 页数, 已并回页数)。
func runCheckpoint(t *testing.T, db *sql.DB, mode string) (int64, int64, int64) {
	t.Helper()
	var busy, logPages, ckptPages int64
	if err := db.QueryRow(`PRAGMA wal_checkpoint(`+mode+`)`).
		Scan(&busy, &logPages, &ckptPages); err != nil {
		t.Fatalf("wal_checkpoint(%s) 失败：%v", mode, err)
	}
	return busy, logPages, ckptPages
}

// walRealCommitThroughput：真磁盘上「每请求写一行」的单价。
//
// 本机测过 1KB 行自动提交 10 355 次/秒，但那是容器 overlayfs；生产磁盘的
// 数字只有这里有。SaveChatLog 现在是响应前同步 INSERT，这笔账直接决定
// 「要不要把日志写入挪出响应路径」——数字小到不需要，就不动契约。
// 探针表用完即删，不碰业务表。
func walRealCommitThroughput(t *testing.T, work string) {
	db := openSQL(t, walRealDSN(work), 1)
	defer db.Close()
	requireWALMode(t, db)

	if _, err := db.Exec(`DROP TABLE IF EXISTS wal_probe`); err != nil {
		t.Fatalf("清理探针表失败：%v", err)
	}
	if _, err := db.Exec(`CREATE TABLE wal_probe (id INTEGER PRIMARY KEY, payload TEXT)`); err != nil {
		t.Fatalf("建探针表失败：%v", err)
	}
	t.Cleanup(func() {
		db.Exec(`DROP TABLE wal_probe`)
	})

	payload := strings.Repeat("x", 1024)
	const n = 2000
	started := time.Now()
	for i := 0; i < n; i++ {
		if _, err := db.Exec(`INSERT INTO wal_probe (payload) VALUES (?)`, payload); err != nil {
			t.Fatalf("第 %d 次提交失败：%v", i+1, err)
		}
	}
	perCommit := time.Since(started) / n
	t.Logf("【吞吐】%d 次 1KB 自动提交（WAL+NORMAL，单连接）：共 %s，%.0f 次/秒，%s/次",
		n, time.Since(started).Round(time.Millisecond),
		float64(n)/time.Since(started).Seconds(), perCommit.Round(time.Microsecond))
	t.Logf("【-wal】跑完之后 %s", humanBytesReal(walFileSize(work+"-wal")))
}

// walRealRollbackToDelete：第七节第 7 条——回退演练。
//
// journal_mode 是库文件头里的属性：翻回 DELETE 必须在没有活跃连接时做，
// 而且做完之后旧的 -wal 要被截干净，写入要照常工作。子测试排在最后，
// 正是因为前面所有连接此时都已关闭。
func walRealRollbackToDelete(t *testing.T, work string) {
	db := openSQL(t, work, 1)
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode=DELETE`).Scan(&mode); err != nil {
		t.Fatalf("翻回 DELETE 失败（有活跃连接时这条会失败）：%v", err)
	}
	if mode != "delete" {
		t.Fatalf("PRAGMA 返回 %q，库没有退回 delete", mode)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("关连接失败：%v", err)
	}

	// 重开验证：模式确实是 delete，-wal 截干净，写入照常。
	db = openSQL(t, work, 1)
	defer db.Close()
	if journalMode(t, work) != "delete" {
		t.Fatal("重开后 journal_mode 不是 delete")
	}
	walAfter := walFileSize(work + "-wal")
	t.Logf("【回退】journal_mode=delete，-wal 截到 %s", humanBytesReal(walAfter))
	if walAfter != 0 {
		t.Fatalf("回退后 -wal 还剩 %d 字节——回退没有收干净", walAfter)
	}

	victim := lastRowID(t, db)
	if _, err := db.Exec(`UPDATE chat_ios SET updated_at = ? WHERE id = ?`,
		time.Now().Format("2006-01-02 15:04:05"), victim); err != nil {
		t.Fatalf("回退后写入失败：%v", err)
	}
	t.Logf("【回退】delete 模式下写入正常（chat_ios.id=%d）", victim)
}

// TestWAL_RealCopyVacuumOnWAL：第七节第 5 条——VACUUM / auto_vacuum 转换在
// WAL 库上的行为。VACUUM 会把整库重写一遍（7 GiB 上是分钟级），默认跳过，
// 显式点名才跑：
//
//	$env:LLMIO_REAL_DB_COPY="D:\llmio-test\work-wal\real.db"
//	$env:LLMIO_WAL_REAL_VACUUM="on"
//	go test ./service/ -run TestWAL_RealCopyVacuumOnWAL -v -timeout 180m
//
// 跑的就是 DB_AUTO_VACUUM_REBUILD 那条路（storage.go 同款：先设
// auto_vacuum，再 VACUUM），量时长、文件大小、freelist，末尾钉住完整性。
func TestWAL_RealCopyVacuumOnWAL(t *testing.T) {
	if os.Getenv("LLMIO_WAL_REAL_VACUUM") != "on" {
		t.Skip("未设置 LLMIO_WAL_REAL_VACUUM=on，跳过（VACUUM 要整库重写一遍）")
	}
	src := realDBForMigration(t)

	work := copyForMigrationNamed(t, src, "work-wal-vacuum", "vacuum.db")
	t.Cleanup(func() {
		_ = os.Remove(work)
		_ = os.Remove(work + "-wal")
		_ = os.Remove(work + "-shm")
	})

	db := openSQL(t, walRealDSN(work), 1)
	defer db.Close()
	requireWALMode(t, db)

	var freelistBefore int64
	if err := db.QueryRow(`PRAGMA freelist_count`).Scan(&freelistBefore); err != nil {
		t.Fatalf("读 freelist 失败：%v", err)
	}
	sizeBefore := fileSize(t, work)
	t.Logf("【前】%s，freelist %d 页，journal_mode=%s",
		humanBytesReal(sizeBefore), freelistBefore, journalMode(t, work))

	// DB_AUTO_VACUUM_REBUILD=on 的同款序列：设 auto_vacuum，VACUUM 应用。
	if _, err := db.Exec(`PRAGMA auto_vacuum = INCREMENTAL`); err != nil {
		t.Fatalf("设 auto_vacuum 失败：%v", err)
	}
	started := time.Now()
	if _, err := db.Exec(`VACUUM`); err != nil {
		t.Fatalf("WAL 库上 VACUUM 失败：%v", err)
	}
	elapsed := time.Since(started).Round(time.Millisecond)

	var freelistAfter int64
	if err := db.QueryRow(`PRAGMA freelist_count`).Scan(&freelistAfter); err != nil {
		t.Fatalf("读 freelist 失败：%v", err)
	}
	sizeAfter := fileSize(t, work)
	t.Logf("【后】%s（Δ %s），freelist %d 页，VACUUM 用了 %s，-wal %s",
		humanBytesReal(sizeAfter), humanBytesReal(sizeAfter-sizeBefore),
		freelistAfter, elapsed, humanBytesReal(walFileSize(work+"-wal")))

	// 硬判据只有两个：VACUUM 没把库写坏；WAL 模式没被 VACUUM 冲掉。
	var integrity string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatalf("完整性自检跑不起来：%v", err)
	}
	if integrity != "ok" {
		t.Fatalf("VACUUM 之后库是坏的：%s", integrity)
	}
	if journalMode(t, work) != "wal" {
		t.Fatal("VACUUM 之后 journal_mode 不再是 wal——迁移脚本得知道这件事")
	}
}
