package balancers

import (
	"errors"
	"testing"
	"time"
)

type spyBalancer struct {
	nextKey   uint
	popErr    error
	keys      []uint // 非空时 Pop 依次返回队列中的候选，用于模拟多候选与重复候选
	deletes   []uint
	reduces   []uint
	successes []uint
}

func (s *spyBalancer) Pop() (uint, error) {
	if len(s.keys) > 0 {
		key := s.keys[0]
		s.keys = s.keys[1:]
		return key, nil
	}
	if s.popErr != nil {
		return 0, s.popErr
	}
	return s.nextKey, nil
}

func (s *spyBalancer) Delete(key uint) { s.deletes = append(s.deletes, key) }
func (s *spyBalancer) Reduce(key uint) { s.reduces = append(s.reduces, key) }
func (s *spyBalancer) Success(key uint) {
	s.successes = append(s.successes, key)
}

// Empty 满足 Balancer 接口新增的方法：spy 始终当作"池里还有候选"。
func (s *spyBalancer) Empty() bool { return false }

func resetBreakerState(t *testing.T) {
	t.Helper()
	mu.Lock()
	nodes = make(map[uint]*Node)
	mu.Unlock()
}

func withBreakerConfig(t *testing.T, maxFailures int, sleepWindow time.Duration, maxRequests int) {
	t.Helper()
	oldMaxFailures := MaxFailures
	oldSleepWindow := SleepWindow
	oldMaxRequests := MaxRequests

	MaxFailures = maxFailures
	SleepWindow = sleepWindow
	MaxRequests = maxRequests

	t.Cleanup(func() {
		MaxFailures = oldMaxFailures
		SleepWindow = oldSleepWindow
		MaxRequests = oldMaxRequests
	})
}

// nodeState 读取指定 key 的熔断状态，要求节点必须已初始化。
func nodeState(t *testing.T, key uint) State {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	node, ok := nodes[key]
	if !ok {
		t.Fatalf("expected node for key %d to be initialized", key)
	}
	return node.state
}

func TestBreakerPopInitializesNode(t *testing.T) {
	resetBreakerState(t)
	withBreakerConfig(t, 3, 50*time.Millisecond, 2)

	spy := &spyBalancer{nextKey: 42}
	breaker := BalancerWrapperBreaker(spy)

	key, err := breaker.Pop()
	if err != nil {
		t.Fatalf("Pop() unexpected error: %v", err)
	}
	if key != 42 {
		t.Fatalf("Pop() key = %d, want %d", key, 42)
	}

	mu.Lock()
	node, ok := nodes[42]
	mu.Unlock()
	if !ok {
		t.Fatalf("expected node for key %d to be initialized", 42)
	}
	if node.state != StateClosed {
		t.Fatalf("node.state = %v, want %v", node.state, StateClosed)
	}
	if node.failCount != 0 || node.successCount != 0 {
		t.Fatalf("node counts = (fail=%d, success=%d), want both 0", node.failCount, node.successCount)
	}
}

func TestBreakerDeleteTripsToOpen(t *testing.T) {
	resetBreakerState(t)
	withBreakerConfig(t, 3, 80*time.Millisecond, 2)

	spy := &spyBalancer{nextKey: 7}
	breaker := BalancerWrapperBreaker(spy)

	if _, err := breaker.Pop(); err != nil {
		t.Fatalf("Pop() unexpected error: %v", err)
	}

	breaker.Delete(7)
	breaker.Delete(7)

	mu.Lock()
	stateAfterTwo := nodes[7].state
	mu.Unlock()
	if stateAfterTwo != StateClosed {
		t.Fatalf("after 2 failures, state = %v, want %v", stateAfterTwo, StateClosed)
	}

	beforeTrip := time.Now()
	breaker.Delete(7)

	mu.Lock()
	node := nodes[7]
	mu.Unlock()
	if node.state != StateOpen {
		t.Fatalf("after %d failures, state = %v, want %v", MaxFailures, node.state, StateOpen)
	}
	if node.expiry.Before(beforeTrip) {
		t.Fatalf("expiry = %v, expected after %v", node.expiry, beforeTrip)
	}
	if len(spy.deletes) != 3 {
		t.Fatalf("underlying Delete calls = %d, want %d", len(spy.deletes), 3)
	}
}

// 构造函数不得做全量扫描：任何预置状态（包括未剔除的 Open 节点）在构造后
// 都必须原样保留，剔除与冷却到期迁移全部延迟到 Pop 命中该节点时进行。
func TestBreakerWrapperDoesNotScan(t *testing.T) {
	cases := []struct {
		name      string
		node      *Node
		wantState State
	}{
		{"open 未过期不剔除", new(Node{state: StateOpen, expiry: time.Now().Add(5 * time.Second)}), StateOpen},
		{"open 已过期不迁移", new(Node{state: StateOpen, expiry: time.Now().Add(-1 * time.Second)}), StateOpen},
		{"halfopen 不动", new(Node{state: StateHalfOpen}), StateHalfOpen},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetBreakerState(t)
			withBreakerConfig(t, 3, 200*time.Millisecond, 2)

			mu.Lock()
			nodes[1] = tc.node
			mu.Unlock()

			spy := &spyBalancer{nextKey: 1}
			_ = BalancerWrapperBreaker(spy)

			if len(spy.deletes) != 0 {
				t.Fatalf("constructor Delete calls = %v, want none", spy.deletes)
			}
			if state := nodeState(t, 1); state != tc.wantState {
				t.Fatalf("node.state = %v, want %v", state, tc.wantState)
			}
		})
	}
}

// Pop 的惰性状态迁移与 Open 节点跳过：表驱动覆盖各状态的放行/剔除/迁移路径。
func TestBreakerPopLazyTransitions(t *testing.T) {
	errNoItems := errors.New("no provide items")

	cases := []struct {
		name   string
		seed   func(t *testing.T) // 预置全局 nodes 状态，可为 nil
		keys   []uint             // spy 依次返回的候选
		popErr error              // 候选耗尽后返回的错误
		verify func(t *testing.T, spy *spyBalancer, key uint, err error)
	}{
		{
			name: "closed 直接放行并惰性初始化节点",
			keys: []uint{7},
			verify: func(t *testing.T, spy *spyBalancer, key uint, err error) {
				if err != nil {
					t.Fatalf("Pop() unexpected error: %v", err)
				}
				if key != 7 {
					t.Fatalf("Pop() key = %d, want %d", key, 7)
				}
				if state := nodeState(t, 7); state != StateClosed {
					t.Fatalf("node.state = %v, want %v", state, StateClosed)
				}
				if len(spy.deletes) != 0 {
					t.Fatalf("Delete calls = %v, want none", spy.deletes)
				}
			},
		},
		{
			name: "open 未过期被跳过并移出本请求候选池",
			seed: func(t *testing.T) {
				mu.Lock()
				defer mu.Unlock()
				nodes[1] = new(Node{state: StateOpen, expiry: time.Now().Add(5 * time.Second)})
			},
			keys: []uint{1, 2},
			verify: func(t *testing.T, spy *spyBalancer, key uint, err error) {
				if err != nil {
					t.Fatalf("Pop() unexpected error: %v", err)
				}
				if key != 2 {
					t.Fatalf("Pop() key = %d, want %d (open node 1 should be skipped)", key, 2)
				}
				if len(spy.deletes) != 1 || spy.deletes[0] != 1 {
					t.Fatalf("Delete calls = %v, want [1]", spy.deletes)
				}
				if state := nodeState(t, 1); state != StateOpen {
					t.Fatalf("node 1 state = %v, want %v (unexpired open must not migrate)", state, StateOpen)
				}
			},
		},
		{
			name: "open 未过期且候选耗尽时透传底层错误",
			seed: func(t *testing.T) {
				mu.Lock()
				defer mu.Unlock()
				nodes[1] = new(Node{state: StateOpen, expiry: time.Now().Add(5 * time.Second)})
			},
			keys:   []uint{1},
			popErr: errNoItems,
			verify: func(t *testing.T, spy *spyBalancer, key uint, err error) {
				if !errors.Is(err, errNoItems) {
					t.Fatalf("Pop() err = %v, want %v", err, errNoItems)
				}
				if len(spy.deletes) != 1 || spy.deletes[0] != 1 {
					t.Fatalf("Delete calls = %v, want [1]", spy.deletes)
				}
				if state := nodeState(t, 1); state != StateOpen {
					t.Fatalf("node 1 state = %v, want %v", state, StateOpen)
				}
			},
		},
		{
			name: "open 过期在 Pop 时惰性迁移 halfopen",
			seed: func(t *testing.T) {
				mu.Lock()
				defer mu.Unlock()
				nodes[1] = new(Node{state: StateOpen, expiry: time.Now().Add(-1 * time.Second)})
			},
			keys: []uint{1},
			verify: func(t *testing.T, spy *spyBalancer, key uint, err error) {
				if err != nil {
					t.Fatalf("Pop() unexpected error: %v", err)
				}
				if key != 1 {
					t.Fatalf("Pop() key = %d, want %d (expired open should be allowed to probe)", key, 1)
				}
				if len(spy.deletes) != 0 {
					t.Fatalf("Delete calls = %v, want none", spy.deletes)
				}
				mu.Lock()
				node := nodes[1]
				mu.Unlock()
				if node.state != StateHalfOpen {
					t.Fatalf("node.state = %v, want %v", node.state, StateHalfOpen)
				}
				if node.failCount != 0 || node.successCount != 0 {
					t.Fatalf("node counts = (fail=%d, success=%d), want both 0", node.failCount, node.successCount)
				}
			},
		},
		{
			name: "halfopen 直接放行",
			seed: func(t *testing.T) {
				mu.Lock()
				defer mu.Unlock()
				nodes[1] = new(Node{state: StateHalfOpen})
			},
			keys: []uint{1},
			verify: func(t *testing.T, spy *spyBalancer, key uint, err error) {
				if err != nil {
					t.Fatalf("Pop() unexpected error: %v", err)
				}
				if key != 1 {
					t.Fatalf("Pop() key = %d, want %d", key, 1)
				}
				if len(spy.deletes) != 0 {
					t.Fatalf("Delete calls = %v, want none", spy.deletes)
				}
				if state := nodeState(t, 1); state != StateHalfOpen {
					t.Fatalf("node.state = %v, want %v", state, StateHalfOpen)
				}
			},
		},
		{
			name: "底层重复给出同一 open 候选时终止循环",
			seed: func(t *testing.T) {
				mu.Lock()
				defer mu.Unlock()
				nodes[1] = new(Node{state: StateOpen, expiry: time.Now().Add(5 * time.Second)})
			},
			// keys 耗尽后 spy 退回 nextKey=1，模拟未按约定缩小候选池的 balancer
			keys: []uint{1},
			verify: func(t *testing.T, spy *spyBalancer, key uint, err error) {
				if err == nil {
					t.Fatal("Pop() expected error for repeated open candidate")
				}
				if len(spy.deletes) != 1 {
					t.Fatalf("Delete calls = %v, want exactly [1]", spy.deletes)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetBreakerState(t)
			withBreakerConfig(t, 3, 200*time.Millisecond, 2)
			if tc.seed != nil {
				tc.seed(t)
			}

			spy := &spyBalancer{keys: tc.keys, popErr: tc.popErr, nextKey: 1}
			breaker := BalancerWrapperBreaker(spy)

			key, err := breaker.Pop()
			tc.verify(t, spy, key, err)
		})
	}
}

// HalfOpen 状态的探测迁移：失败立刻回 Open，成功达标后关闭熔断。
func TestBreakerHalfOpenTransitions(t *testing.T) {
	cases := []struct {
		name   string
		run    func(t *testing.T, breaker *Breaker, spy *spyBalancer)
		verify func(t *testing.T, spy *spyBalancer)
	}{
		{
			name: "halfopen 失败立刻回 open",
			run: func(t *testing.T, breaker *Breaker, spy *spyBalancer) {
				before := time.Now()
				breaker.Delete(7)

				mu.Lock()
				node := nodes[7]
				mu.Unlock()
				if node.state != StateOpen {
					t.Fatalf("after half-open failure, state = %v, want %v", node.state, StateOpen)
				}
				if node.expiry.Before(before) {
					t.Fatalf("expiry = %v, expected after %v", node.expiry, before)
				}
			},
			verify: func(t *testing.T, spy *spyBalancer) {
				if len(spy.deletes) != 1 || spy.deletes[0] != 7 {
					t.Fatalf("underlying Delete calls = %v, want [7]", spy.deletes)
				}
			},
		},
		{
			name: "halfopen 成功达标关闭",
			run: func(t *testing.T, breaker *Breaker, spy *spyBalancer) {
				breaker.Success(7)
				if state := nodeState(t, 7); state != StateHalfOpen {
					t.Fatalf("after 1 success, state = %v, want %v", state, StateHalfOpen)
				}

				breaker.Success(7)
				mu.Lock()
				node := nodes[7]
				mu.Unlock()
				if node.state != StateClosed {
					t.Fatalf("after %d successes, state = %v, want %v", MaxRequests, node.state, StateClosed)
				}
				if node.successCount != 0 {
					t.Fatalf("successCount = %d, want 0 after reset", node.successCount)
				}
			},
			verify: func(t *testing.T, spy *spyBalancer) {
				if len(spy.successes) != 2 {
					t.Fatalf("underlying Success calls = %d, want %d", len(spy.successes), 2)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetBreakerState(t)
			withBreakerConfig(t, 3, 200*time.Millisecond, 2)

			mu.Lock()
			nodes[7] = new(Node{state: StateHalfOpen})
			mu.Unlock()

			spy := &spyBalancer{nextKey: 7}
			breaker := BalancerWrapperBreaker(spy)

			tc.run(t, breaker, spy)
			tc.verify(t, spy)
		})
	}
}
