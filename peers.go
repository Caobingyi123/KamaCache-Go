package kamacache

// 负责路由，即如果key不在本地，应该去哪里找
import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/youngyangyang04/KamaCache-Go/consistenthash"
	"github.com/youngyangyang04/KamaCache-Go/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 默认服务名称
const defaultSvcName = "kama-cache"

// PeerPicker 定义了peer选择器的接口
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}

// Peer 定义了缓存节点的接口
type Peer interface {
	Get(group string, key string) ([]byte, error)
	// 给远程节点设置缓存
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(group string, key string) (bool, error)
	Close() error
}

// ClientPicker 实现了PeerPicker接口
type ClientPicker struct {
	selfAddr string // 当前节点地址
	svcName  string // 服务名称
	mu       sync.RWMutex
	consHash *consistenthash.Map // 一致性哈希，存的是节点的地址
	clients  map[string]*Client  // 地址到gRPC Client的映射
	etcdCli  *clientv3.Client
	ctx      context.Context
	cancel   context.CancelFunc
}

// PickerOption 定义配置选项
type PickerOption func(*ClientPicker)

// WithServiceName 设置服务名称
func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

// PrintPeers 打印当前已发现的节点（仅用于调试）
func (p *ClientPicker) PrintPeers() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("当前已发现的节点:")
	for addr := range p.clients {
		log.Printf("- %s", addr)
	}
}

// NewClientPicker 创建新的ClientPicker实例
func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		selfAddr: addr,
		svcName:  defaultSvcName,
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
		ctx:      ctx,
		cancel:   cancel,
	}

	for _, opt := range opts {
		opt(picker)
	}
	// 创建etcd客户端
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   registry.DefaultConfig.Endpoints,
		DialTimeout: registry.DefaultConfig.DialTimeout,
	})
	if err != nil {
		cancel() // 释放资源
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}
	picker.etcdCli = cli

	// 启动服务发现
	if err := picker.startServiceDiscovery(); err != nil {
		cancel()
		cli.Close()
		return nil, err
	}

	return picker, nil
}

// startServiceDiscovery 启动服务发现
func (p *ClientPicker) startServiceDiscovery() error {
	// 先进行全量更新(先拉去所有存在的节点)
	if err := p.fetchAllServices(); err != nil {
		return err
	}

	// 然后监听后续的新增和删除的节点x
	go p.watchServiceChanges()
	return nil
}

// watchServiceChanges 监听服务实例变化
func (p *ClientPicker) watchServiceChanges() {
	//创建一个etcd的watcher
	watcher := clientv3.NewWatcher(p.etcdCli)
	// 监听前缀为"/services/"+p.svcName的key，只要有修改就会往这个chan里面发信息
	watchChan := watcher.Watch(p.ctx, "/services/"+p.svcName, clientv3.WithPrefix())

	for {
		select {
		case <-p.ctx.Done(): // 如果外面调用了p.canel()
			watcher.Close()
			return
		case resp := <-watchChan: //如果etch有事件，那么就会往这个chan里面发信息
			p.handleWatchEvents(resp.Events)
		}
	}
}

// handleWatchEvents 处理监听到的事件
func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, event := range events {
		addr := string(event.Kv.Value) // 取地址
		if addr == p.selfAddr {
			continue
		}

		switch event.Type {
		case clientv3.EventTypePut:
			if _, exists := p.clients[addr]; !exists {
				p.set(addr)
				logrus.Infof("New service discovered at %s", addr)
			}
		case clientv3.EventTypeDelete:
			if client, exists := p.clients[addr]; exists {
				client.Close() //关闭client
				p.remove(addr) // 从一致性哈希中移除
				logrus.Infof("Service removed at %s", addr)
			}
		}
	}
}

// fetchAllServices 获取所有服务实例
func (p *ClientPicker) fetchAllServices() error {
	// 先创建一个3秒超时的context
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()
	//从etcd查询所有的服务节点
	// WithPrefix的意思是查询所有以"/services/"+p.svcName,开头的key
	resp, err := p.etcdCli.Get(ctx, "/services/"+p.svcName, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get all services: %v", err)
	}
	// 加写锁
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, kv := range resp.Kvs {
		addr := string(kv.Value)              // 拿到服务地址
		if addr != "" && addr != p.selfAddr { // 不为空且不是自己
			p.set(addr)
			logrus.Infof("Discovered service at %s", addr)
		}
	}
	return nil
}

// set 添加服务实例
func (p *ClientPicker) set(addr string) {
	if client, err := NewClient(addr, p.svcName, p.etcdCli); err == nil {
		p.consHash.Add(addr)
		p.clients[addr] = client
		logrus.Infof("Successfully created client for %s", addr)
	} else {
		logrus.Errorf("Failed to create client for %s: %v", addr, err)
	}
}

// remove 移除服务实例
func (p *ClientPicker) remove(addr string) {
	p.consHash.Remove(addr) // 从一致性哈希里删除
	delete(p.clients, addr) // 从map中删除链接
}

// PickPeer 根据key选择远程的peer节点
func (p *ClientPicker) PickPeer(key string) (Peer, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if addr := p.consHash.Get(key); addr != "" {
		if client, ok := p.clients[addr]; ok {
			return client, true, addr == p.selfAddr
		}
	}
	return nil, false, false
}

// Close 关闭所有资源
func (p *ClientPicker) Close() error {
	p.cancel() //取消p.ctx，调用取消的信号，触发p.ctx.Done
	p.mu.Lock()
	defer p.mu.Unlock()

	// 错误切片，多个错误
	var errs []error
	for addr, client := range p.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", addr, err))
		}
	}

	if err := p.etcdCli.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}
	return nil
}

// parseAddrFromKey 从etcd key中解析地址
// 路径大概services/kama-cache/127.0.0.1:8001 要拿到最后的127....
func parseAddrFromKey(key, svcName string) string {
	prefix := fmt.Sprintf("/services/%s/", svcName)
	if strings.HasPrefix(key, prefix) { // 判断key是否已prefix开头
		return strings.TrimPrefix(key, prefix) // 去掉前缀
	}
	return ""
}
