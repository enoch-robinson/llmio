package models

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/atopos31/llmio/pkg/sqlitex"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// 这个文件盯的是 DB_WAL 这个开关本身，以及它凭什么值得存在。
// 结论与限制（含没能在本机验证的部分）见 docs/db-wal-evaluation.md。

func TestSQLiteDSN(t *testing.T) {
	cases := []struct {
		name string
		path string
		wal  bool
		want string
	}{
		{
			name: "默认只钉事务锁类型",
			path: "./db/llmio.db",
			wal:  false,
			want: "./db/llmio.db?_txlock=immediate",
		},
		{
			name: "路径已带查询串时用 & 续接",
			path: "./db/llmio.db?cache=shared",
			wal:  false,
			want: "./db/llmio.db?cache=shared&_txlock=immediate",
		},
		{
			name: "WAL 追加三条连接级参数",
			path: "./db/llmio.db",
			wal:  true,
			want: "./db/llmio.db?_txlock=immediate" +
				"&_pragma=journal_mode(WAL)" +
				"&_pragma=synchronous(NORMAL)" +
				"&_pragma=busy_timeout(5000)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sqliteDSN(tc.path, tc.wal); got != tc.want {
				t.Fatalf("DSN 不对：\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

// WAL 是**写进库文件头**的属性，不是进程的配置：撤掉开关重启，库仍是 WAL。
// JournalMode 必须反映这件事，否则 DBFileSize 会在"其实是 WAL"的库上漏算 -wal。
func TestInitWALIsPersistentAndReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal-persist.db")

	t.Setenv(WALEnv, "true")
	Init(context.Background(), path)
	closeCurrentDBOnCleanup(t)
	if !WALEnabled() || JournalMode != "wal" {
		t.Fatalf("开关打开后库没进 WAL：journal_mode=%q", JournalMode)
	}

	closeCurrentDB(t)
	t.Setenv(WALEnv, "false")
	Init(context.Background(), path)
	closeCurrentDBOnCleanup(t)
	if !WALEnabled() {
		t.Fatalf("WAL 是库的属性，撤掉开关后不该退回 delete：journal_mode=%q", JournalMode)
	}
}

// WAL 的核心收益：读事务不再挡住写者。这条用例把两种模式放在同一场景下对比，
// 既证明开关真的生效，也给出"收益有多大"在驱动层面的直接证据（delete 模式下
// 写者要等满 busy_timeout 才失败）。
func TestWALReadTransactionDoesNotBlockWriter(t *testing.T) {
	sqlitex.Register()
	const busyMS = 200

	cases := []struct {
		name    string
		wal     bool
		wantErr bool
	}{
		{name: "delete 模式下读事务挡住写", wal: false, wantErr: true},
		{name: "WAL 下读事务不挡写", wal: true, wantErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal-lock.db")
			// 写连接走生产用的 DSN（含 _txlock=immediate）。
			dsn := sqliteDSN(path, tc.wal)
			// 读连接**不能**带 _txlock=immediate：那会让 BEGIN 也去抢写锁，
			// 于是"读者"实际上是另一个写者，这条用例就验不到"读不挡写"了。
			readDSN := deferDSN(path, tc.wal, busyMS)
			writeDSN := dsn + "&_pragma=busy_timeout(" + strconv.Itoa(busyMS) + ")"

			setup := openWALTestDB(t, dsn)
			if err := setup.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`).Error; err != nil {
				t.Fatalf("建表失败：%v", err)
			}
			if err := setup.Exec(`INSERT INTO t (v) VALUES ('seed')`).Error; err != nil {
				t.Fatalf("种子行写入失败：%v", err)
			}

			// 读连接：开事务后把结果集挂住不关，读事务一直在这个连接上。
			// WAL 下它持的是快照（read mark）而不是文件锁，所以挡不住写者。
			reader := openWALTestDB(t, readDSN)
			sqlDB, err := reader.DB()
			if err != nil {
				t.Fatalf("取读连接失败：%v", err)
			}
			tx, err := sqlDB.Begin()
			if err != nil {
				t.Fatalf("开读事务失败：%v", err)
			}
			rows, err := tx.Query(`SELECT id, v FROM t`)
			if err != nil {
				t.Fatalf("读失败：%v", err)
			}
			if !rows.Next() {
				t.Fatal("种子行读不出来，读事务没真正拿到锁")
			}
			// 故意不 Close rows / 不结束事务：这就是"一次长读"的现场。

			writer := openWALTestDB(t, writeDSN)
			start := time.Now()
			writeErr := writer.Exec(`INSERT INTO t (v) VALUES ('during-read')`).Error
			elapsed := time.Since(start)
			t.Logf("读事务持锁时写入耗时 %s（busy_timeout=%dms），err=%v", elapsed, busyMS, writeErr)

			_ = rows.Close()
			_ = tx.Rollback()

			if tc.wantErr {
				if writeErr == nil {
					t.Fatal("delete 模式下读事务应当挡住写者，这笔写入却成功了")
				}
				if elapsed < busyMS/2*time.Millisecond {
					t.Fatalf("应当等满 busy_timeout 才失败，实际只用了 %s", elapsed)
				}
				return
			}
			if writeErr != nil {
				t.Fatalf("WAL 下读事务不该挡住写者：%v", writeErr)
			}
		})
	}
}

// deferDSN 造一个**延迟事务**的读连接：不带 _txlock=immediate。
// 只有这样才能模拟"普通查询"拿到的读锁——生产里的单条查询与 Raw().Row()
// 就是这条路（GORM 不为单条查询开事务）。
func deferDSN(path string, wal bool, busyMS int) string {
	dsn := path + "?_pragma=busy_timeout(" + strconv.Itoa(busyMS) + ")"
	if wal {
		dsn += "&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	}
	return dsn
}

func openWALTestDB(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Dialector{DriverName: sqlitex.DriverName, DSN: dsn}, &gorm.Config{})
	if err != nil {
		t.Fatalf("打开测试库失败：%v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}
