package balancers

import (
	"fmt"
	"sync"
	"time"
)

type State int

const (
	StateClosed   State = iota // 正常运行
	StateOpen                  // 熔断触发
	StateHalfOpen              // 探测恢复
)

type Node struct {
	state        State     // 熔断状态
	failCount    int       // 失败次数
	successCount int       // 成功次数
	expiry       time.Time // 冷却结束时间
}

func (n *Node) Reset(state State) {
	n.state = state
	n.failCount = 0
	n.successCount = 0
}

var (
	mu          sync.Mutex
	nodes       = make(map[uint]*Node)
	MaxFailures = 5                // 最多失败次数
	SleepWindow = 60 * time.Second // 冷却时间
	MaxRequests = 2                // 在 HalfOpen 状态下, 如果请求成功次数超过此数值，熔断器关闭（恢复）；如果有一个失败，重新进入 Open 状态
)

type Breaker struct {
	Balancer
}

// BalancerWrapperBreaker 按请求包装；构造刻意不做全量扫描（全局锁会串行化
// 全进程请求），Open 剔除与冷却到期迁移都延迟到 Pop 命中该节点时进行。
func BalancerWrapperBreaker(balancer Balancer) *Breaker {
	return &Breaker{Balancer: balancer}
}

// Pop 跳过熔断中的节点：冷却到期的 Open 在锁内迁移为 HalfOpen 并放行探测。
func (b *Breaker) Pop() (uint, error) {
	var skipped map[uint]struct{}
	for {
		key, err := b.Balancer.Pop()
		if err != nil {
			return 0, err
		}
		if b.allow(key) {
			return key, nil
		}
		if skipped == nil {
			skipped = make(map[uint]struct{})
		}
		if _, seen := skipped[key]; seen {
			// 底层未按约定在 Delete 后缩小候选池，终止避免死循环
			return 0, fmt.Errorf("all candidate nodes are in open state")
		}
		skipped[key] = struct{}{}
		// 只移出本次调度，不计入失败
		b.Balancer.Delete(key)
	}
}

// allow 在锁内完成该节点的惰性状态迁移。
func (b *Breaker) allow(key uint) bool {
	mu.Lock()
	defer mu.Unlock()
	node, ok := nodes[key]
	if !ok {
		nodes[key] = new(Node{state: StateClosed})
		return true
	}
	if node.state == StateOpen && node.expiry.Before(time.Now()) {
		node.Reset(StateHalfOpen)
		return true
	}
	return node.state != StateOpen
}

func (b *Breaker) Delete(key uint) {
	b.failCountAdd(key)
	b.Balancer.Delete(key)
}

func (b *Breaker) Reduce(key uint) {
	b.failCountAdd(key)
	b.Balancer.Reduce(key)
}

func (b *Breaker) failCountAdd(key uint) {
	mu.Lock()
	defer mu.Unlock()
	if node, ok := nodes[key]; ok {
		node.failCount += 1
		if node.state == StateClosed && node.failCount >= MaxFailures {
			node.Reset(StateOpen)
			node.expiry = time.Now().Add(SleepWindow)
		}

		if node.state == StateHalfOpen {
			node.Reset(StateOpen)
			node.expiry = time.Now().Add(SleepWindow)
		}
	}
}

func (b *Breaker) Success(key uint) {
	mu.Lock()
	defer mu.Unlock()
	if node, ok := nodes[key]; ok {
		if node.state == StateHalfOpen {
			node.successCount += 1
			if node.successCount >= MaxRequests {
				node.Reset(StateClosed)
			}
		}
	}
	b.Balancer.Success(key)
}
