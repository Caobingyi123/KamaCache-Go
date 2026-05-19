package consistenthash

import "hash/crc32"

// Config 一致性哈希配置
type Config struct {
	// 每个真实节点对应的虚拟节点数
	DefaultReplicas int
	// 最小虚拟节点数
	MinReplicas int
	// 最大虚拟节点数
	MaxReplicas int
	// 哈希函数
	HashFunc func(data []byte) uint32
	// 负载均衡阈值，超过此值触发虚拟节点调整
	LoadBalanceThreshold float64
}

// DefaultConfig 默认配置
var DefaultConfig = &Config{
	DefaultReplicas: 50, // 虚拟节点
	MinReplicas:     10,
	MaxReplicas:     200,
	HashFunc:        crc32.ChecksumIEEE, // 哈希算法的函数

	//负载均衡阈值---表示负载差距超过 25% 时，后面的 checkAndRebalance 可能会调整虚拟节点数。
	LoadBalanceThreshold: 0.25, // 25% 的负载不均衡度触发调整
}
