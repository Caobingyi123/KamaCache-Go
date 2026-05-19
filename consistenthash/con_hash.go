package consistenthash

// 结果一致性哈希节点问题，也就是一个key应该去哪个节点的问题
import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Map 一致性哈希实现
type Map struct {
	mu sync.RWMutex
	// 配置信息
	config *Config
	// 哈希环，存环的的所有节点的哈希值，包括虚拟节点，且有序
	keys []int
	// 虚拟节点到节点的映射
	hashMap map[int]string
	// 节点到虚拟节点数量的映射
	nodeReplicas map[string]int
	// 节点负载统计-节点被选中次数
	nodeCounts map[string]int64
	// 总请求数
	totalRequests int64
	stopCh        chan struct{}
	stopOnce      sync.Once
}

// New 创建一致性哈希实例
func New(opts ...Option) *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
		stopCh:       make(chan struct{}),
	}

	for _, opt := range opts {
		opt(m)
	}

	m.startBalancer(m.stopCh) // 启动负载均衡器
	return m
}

// Option 配置选项
type Option func(*Map)

// WithConfig 设置配置
// 一般是调用New(WithConfig(myconfig))这种调用，来获取myconfig里面的配置
func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

// Add 添加节点
func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodes {
		if node == "" {
			//跳过空节点
			continue
		}

		// 为节点添加虚拟节点，这里默认是50
		m.addNode(node, m.config.DefaultReplicas)
	}

	// 重新排序
	sort.Ints(m.keys)
	return nil
}

// Remove 移除节点
func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 先插虚拟节点的数量是多少
	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	// 移除节点的所有虚拟节点
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		//  删除虚拟节点和真实的映射
		delete(m.hashMap, hash)
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				// 在环上找到j了，那只删除j的
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}

	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}

// Get 获取节点，传入key返回节点的名称
func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.keys) == 0 {
		return ""
	}
	// 把key变成hash数字
	hash := int(m.config.HashFunc([]byte(key)))
	// 二分查找，在keys里面找到第一个>=keyHash的位置
	idx := sort.Search(len(m.keys), func(i int) bool {
		// 在已排序的哈希环 m.keys 中，找到第一个 >= hash 的虚拟节点位置
		return m.keys[i] >= hash
	})

	// 处理边界情况，就是找不到符合条件的时候就返回开头(因为是个环)
	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	m.nodeCounts[node]++
	m.totalRequests++

	return node
}

// // addNode 添加节点的虚拟节点
// // node 节点名称，replicas 虚拟节点数量

// node = "localhost:8001"
// replicas = 50

// // 生成localhost:8001-0
// localhost:8001-1
// localhost:8001-2。。。
// localhost:8001-49

func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		// 添加虚拟节点
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
}

// checkAndRebalance 检查并重新平衡虚拟节点
func (m *Map) checkAndRebalance() {
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return // 样本太少，不进行调整
	}

	// 计算负载情况，len(m.nodeReplicas)表示真实节点个数
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))
	var maxDiff float64

	for _, count := range m.nodeCounts {
		diff := math.Abs(float64(count) - avgLoad)
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}

	// 如果负载不均衡度超过阈值，调整虚拟节点
	if maxDiff > m.config.LoadBalanceThreshold {
		m.rebalanceNodes()
	}
}

// rebalanceNodes 重新平衡节点
func (m *Map) rebalanceNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 再次计算平均负载
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))

	// 调整每个节点的虚拟节点数量
	for node, count := range m.nodeCounts {
		currentReplicas := m.nodeReplicas[node]
		// 负载比例
		loadRatio := float64(count) / avgLoad

		var newReplicas int
		if loadRatio > 1 {
			// 负载过高，减少虚拟节点
			newReplicas = int(float64(currentReplicas) / loadRatio)
		} else {
			// 负载过低，增加虚拟节点
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		// 确保在限制范围内
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != currentReplicas {
			// 重新添加节点的虚拟节点
			if err := m.Remove(node); err != nil {
				continue // 如果移除失败，跳过这个节点
			}
			// addNode添加虚拟节点
			m.addNode(node, newReplicas)
		}
	}

	// 重置计数器
	for node := range m.nodeCounts {
		m.nodeCounts[node] = 0
	}
	atomic.StoreInt64(&m.totalRequests, 0)

	// 重新排序
	sort.Ints(m.keys)
}

// GetStats 获取负载统计信息
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := m.totalRequests
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeCounts {
		stats[node] = float64(count) / float64(total)
	}
	return stats
}

// 将checkAndRebalance移到单独的goroutine中
func (m *Map) startBalancer(stopCh <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.checkAndRebalance()
			case <-stopCh:
				return
			}
		}
	}()
}

// 给通道传递关闭通知
func (m *Map) Close() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}
