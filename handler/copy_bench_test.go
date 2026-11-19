package handler

import (
	"bytes"
	"io"
	"testing"
)

// 这个基准钉的是"转发响应体时那次 32 KiB 分配"。
//
// 两端都要避开 io.Copy 的快路径：bytes.Reader 实现了 WriteTo、io.Discard 实现了
// ReadFrom，任一方命中快路径都不会分配缓冲，也就量不到要量的东西。生产里的两端
// 是 *io.TeeReader 与 gin 的 responseWriter（都不实现这两个接口）。

type plainReader struct{ r io.Reader }

func (p plainReader) Read(b []byte) (int, error) { return p.r.Read(b) }

type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

func BenchmarkCopyStream(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 64<<10)
	b.SetBytes(int64(len(data)))

	b.Run("io.Copy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var dst countingWriter
			if _, err := io.Copy(&dst, plainReader{bytes.NewReader(data)}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("copyStream", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var dst countingWriter
			if _, err := copyStream(&dst, plainReader{bytes.NewReader(data)}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
