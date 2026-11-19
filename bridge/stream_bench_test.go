package bridge

import (
	"io"
	"strconv"
	"strings"
	"testing"
)

// 流式翻译的基准。
//
// 用例模拟"重文本 delta"的 SSE 流（几千个 chunk，每条带一段正文），覆盖两个翻译
// 方向。每组两条：Bridged* 走 BridgedBody 这条真实转发路径（上游按 4KB 分片喂进
// 来、客户端持续读走），Translate* 直连 Streamer.Write，用于把翻译层开销与 Read
// 循环的开销分开归因。
//
// b.SetBytes 计的是上游输入字节数，输出里的 MB/s 即"每秒能消化多少上游字节"。

const benchDeltaChunks = 4000

// 每条 delta 携带的正文，20 字节 ASCII，避免多字节字符让字节数随内容波动。
const benchDeltaText = `the quick brown fox `

// benchOpenAIStream 拼一条 OpenAI 的重文本 delta 流，结尾带 [DONE]。
func benchOpenAIStream() []byte {
	line := `{"id":"chatcmpl-bench","object":"chat.completion.chunk","created":1700000000,"model":"bench-model","choices":[{"index":0,"delta":{"content":"` + benchDeltaText + `"},"finish_reason":null}]}` + "\n\n"
	var b strings.Builder
	b.Grow((len(line)+len("data: "))*benchDeltaChunks + len("data: [DONE]\n\n"))
	for i := 0; i < benchDeltaChunks; i++ {
		b.WriteString("data: ")
		b.WriteString(line)
	}
	b.WriteString("data: [DONE]\n\n")
	return []byte(b.String())
}

// benchAnthropicStream 拼一条 Anthropic 的重 text_delta 流，开场与收尾事件齐全。
func benchAnthropicStream() []byte {
	delta := `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"` + benchDeltaText + `"}}`
	var b strings.Builder
	b.Grow((len(delta) + 64) * (benchDeltaChunks + 4))
	b.WriteString(encodeSSE("message_start", `{"type":"message_start","message":{"id":"msg_bench","type":"message","role":"assistant","model":"bench-model","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":1}}}`))
	b.WriteString(encodeSSE("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
	for i := 0; i < benchDeltaChunks; i++ {
		b.WriteString(encodeSSE("content_block_delta", delta))
	}
	b.WriteString(encodeSSE("content_block_stop", `{"type":"content_block_stop","index":0}`))
	b.WriteString(encodeSSE("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":`+strconv.Itoa(benchDeltaChunks*len(benchDeltaText))+`}}`))
	b.WriteString(encodeSSE("message_stop", `{"type":"message_stop"}`))
	return []byte(b.String())
}

// drainBridged 把 body 读到 EOF，模拟客户端持续消费。
func drainBridged(b *testing.B, body io.Reader) {
	b.Helper()
	sink := make([]byte, 64<<10)
	for {
		n, err := body.Read(sink)
		if err == io.EOF {
			return
		}
		if err != nil {
			b.Fatal(err)
		}
		if n == 0 {
			b.Fatal("没有错误时不应空读")
		}
	}
}

// benchmarkBridged 走 BridgedBody 转发路径：上游按 4KB 分片（chunkedReader 与
// BridgedBody 内部 4KB 缓冲对齐，模拟真实上游的读块大小）。
func benchmarkBridged(b *testing.B, newStream func() Streamer, input []byte) {
	b.Helper()
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := NewBridgedBody(io.NopCloser(&chunkedReader{data: input, step: 4096}), newStream())
		drainBridged(b, body)
	}
}

// benchmarkTranslate 直连 Streamer，跳过 Read 循环，单独量翻译层。
func benchmarkTranslate(b *testing.B, newStream func() Streamer, input []byte) {
	b.Helper()
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := newStream()
		for off := 0; off < len(input); off += 4096 {
			end := off + 4096
			if end > len(input) {
				end = len(input)
			}
			if _, err := s.Write(input[off:end]); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := s.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenAIToAnthropicBridged(b *testing.B) {
	benchmarkBridged(b, func() Streamer { return NewOpenAIToAnthropicStream() }, benchOpenAIStream())
}

func BenchmarkAnthropicToOpenAIBridged(b *testing.B) {
	benchmarkBridged(b, func() Streamer { return NewAnthropicToOpenAIStream() }, benchAnthropicStream())
}

func BenchmarkOpenAIToAnthropicTranslate(b *testing.B) {
	benchmarkTranslate(b, func() Streamer { return NewOpenAIToAnthropicStream() }, benchOpenAIStream())
}

func BenchmarkAnthropicToOpenAITranslate(b *testing.B) {
	benchmarkTranslate(b, func() Streamer { return NewAnthropicToOpenAIStream() }, benchAnthropicStream())
}
