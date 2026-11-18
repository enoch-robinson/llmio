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

// BalancerWrapperBreaker 按请求包装 balancer。这里刻意不做全量扫描：
// balancer 每个请求都会新建，若在构造时遍历所有节点（旧实现），
// 全局锁 mu 会成为全进程请求的串行化点。Open 节点的剔除与冷却到期
// 迁移改为在 Pop 命中具体节点时惰性完成，锁内工作从 O(全部节点) 降为 O(1)。
func BalancerWrapperBreaker(balancer Balancer) *Breaker {
	return &Breaker{Balancer: balancer}
}

// Pop 抽取候选并跳过熔断中的节点：Open 且未过期的节点从本请求的候选池
// 剔除后继续抽下一个；Open 且已过期（SleepWindow 冷却结束）的节点在锁内
// 迁移为 HalfOpen 并放行探测；其余状态直接放行。候选耗尽时透传底层
// balancer 的既有错误。
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
			// 底层 balancer 若未按约定在 Delete 后缩小候选池（Pop 重复给出
			// 同一节点），及时终止避免死循环。
			return 0, fmt.Errorf("all candidate nodes are in open state")
		}
		skipped[key] = struct{}{}
		// 走底层 Delete：这里只是把熔断节点移出本次调度，不能计入失败。
		b.Balancer.Delete(key)
	}
}

// allow 判断 key 能否参与本次调度，并在锁内完成该节点的惰性状态迁移；
// 首次出现的 key 初始化为 Closed。
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
