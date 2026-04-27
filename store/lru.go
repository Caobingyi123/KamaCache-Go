package store

import (
	"container/list"
	"sync"
	"time"
)

// lruCache 是基于标准库 list 的 LRU 缓存实现
// 这个结构体后面
type lruCache struct {
	mu              sync.RWMutex                  //读写分离锁
	list            *list.List                    // 双向链表，用于维护 LRU 顺序
	items           map[string]*list.Element      // 键到链表节点的映射,就是取出来的值
	expires         map[string]time.Time          // 过期时间映射
	maxBytes        int64                         // 来自store.Options最大允许字节数
	usedBytes       int64                         // 当前使用的字节数
	onEvicted       func(key string, value Value) // 来自store.Options，回调函数
	cleanupInterval time.Duration                 // 来自store.Options，定时清理的间隔
	cleanupTicker   *time.Ticker                  // 定时清理触发器
	closeCh         chan struct{}                 // 用于优雅关闭清理协程
}

// lruEntry 表示缓存中的一个条目，也就是一个节点
type lruEntry struct {
	key   string
	value Value // 在store里面定义的接口，需实现Len方法，统计字节数量
}

// newLRUCache 创建一个新的 LRU 缓存实例
// 参数opts是store.Options
func newLRUCache(opts Options) *lruCache {
	// 设置默认清理间隔，来自store.NewOptions
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute // 默认为 1 分钟
	}

	c := &lruCache{
		list:            list.New(),                     // 初始化双向链表
		items:           make(map[string]*list.Element), // 初始化键到链表节点的映射
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}

	// 启动定期清理协程
	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	//cleanuoLoop定义在后面，是lruCache的方法
	go c.cleanupLoop()

	return c
}

// Get 获取缓存项，如果存在且未过期则返回
func (c *lruCache) Get(key string) (Value, bool) {
	c.mu.RLock()             // 读锁
	elem, ok := c.items[key] //查哈希表，获取元素
	if !ok {                 //不存在直接返回
		c.mu.RUnlock()
		return nil, false
	}

	// 检查是否过期，如果过期就异步删除
	if expTime, hasExp := c.expires[key]; hasExp && time.Now().After(expTime) {
		c.mu.RUnlock()

		// 异步删除过期项，避免在读锁内操作
		go c.Delete(key)

		return nil, false
	}

	// 获取值并释放读锁，elem.Value取出来的是interface{}，需要转换
	entry := elem.Value.(*lruEntry)
	value := entry.value
	c.mu.RUnlock()

	// 更新 LRU 位置需要写锁
	c.mu.Lock()
	// 再次检查元素是否仍然存在（可能在获取写锁期间被其他协程删除）
	if _, ok := c.items[key]; ok {
		c.list.MoveToBack(elem) //把元素移动到链表尾部
	}
	c.mu.Unlock()

	return value, true
}

// Set 添加或更新缓存项，封装了SetWithExpiration，设置永不过期
func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

// SetWithExpiration 添加或更新缓存项，并设置过期时间
func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}

	//加写锁
	c.mu.Lock()
	//自动释放，防止挂掉之后死锁
	defer c.mu.Unlock()

	// 计算过期时间
	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	} else {
		// 永不过期
		delete(c.expires, key)
	}

	// 如果键已存在，更新值
	if elem, ok := c.items[key]; ok {
		oldEntry := elem.Value.(*lruEntry)
		// 如果新值和旧值字节数不一样，需要更新用量
		c.usedBytes += int64(value.Len() - oldEntry.value.Len())
		oldEntry.value = value  //直接原地修改旧节点的值
		c.list.MoveToBack(elem) //移到尾部，表示最近使用
		return nil
	}

	// key不存在的时候，添加新项
	entry := &lruEntry{key: key, value: value}
	elem := c.list.PushBack(entry) //添加到尾部
	c.items[key] = elem            //添加映射
	//总长度等于key+value的长度
	c.usedBytes += int64(len(key) + value.Len())

	// 检查是否需要淘汰旧项
	c.evict()

	return nil
}

// Delete 从缓存中删除指定键的项
func (c *lruCache) Delete(key string) bool {
	c.mu.Lock() //先加写锁
	defer c.mu.Unlock()

	//查找是否存在key，如果存在就删除
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

// Clear 清空缓存
func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果设置了回调函数，遍历所有项调用回调，因为每个被删除的项都有可能需要执行清理逻辑
	if c.onEvicted != nil {
		for _, elem := range c.items {
			entry := elem.Value.(*lruEntry)
			c.onEvicted(entry.key, entry.value)
		}
	}

	c.list.Init()                            //清空双向链表
	c.items = make(map[string]*list.Element) //make新的，旧的就会被GC
	c.expires = make(map[string]time.Time)   //同上
	c.usedBytes = 0
}

// Len 返回缓存中的项数
func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len() //返回键值对的数量，也就是长度
}

// removeElement 从缓存中删除元素，调用此方法前必须持有锁，
// 这个方法是一个私有方法
func (c *lruCache) removeElement(elem *list.Element) {
	//从链表节点中取出缓存项entry（包含 key 和 value）
	//elem是链表节点，entry是节点里的数据
	entry := elem.Value.(*lruEntry)
	c.list.Remove(elem)        //从双向链表中删除该节点
	delete(c.items, entry.key) //删除映射包括过期时间和
	delete(c.expires, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.value.Len()) //更新内存长度

	if c.onEvicted != nil { //触发回调
		c.onEvicted(entry.key, entry.value)
	}
}

// evict 清理过期和超出内存限制的缓存，调用此方法前必须持有锁，也是私有方法。
func (c *lruCache) evict() {
	// 先清理过期项
	now := time.Now()
	for key, expTime := range c.expires {
		//如果过期时间小于当前时间，就删除
		if now.After(expTime) {
			if elem, ok := c.items[key]; ok {
				c.removeElement(elem)
			}
		}
	}

	// 再根据内存限制清理最久未使用的项
	// 如果设置了最大内存限制，且当前使用的内存超过了限制且缓存不为空的时候
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		elem := c.list.Front() // 获取最久未使用的项（链表头部），赋值给 elem，然后给他删掉
		if elem != nil {
			c.removeElement(elem)
		}
	}
}

// cleanupLoop 定期清理过期缓存的协程
func (c *lruCache) cleanupLoop() {
	for {
		select {
		//定时器信号，每隔 cleanupInterval 触发一次
		//.C 是一个 channel，用于接收定时器的信号
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

// Close 关闭缓存，停止清理协程，释放资源避免泄露
func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop() //停止定时器，防止还有其他引用，让lreuCache不能够被GC

		//当一个 channel 被关闭后，任何从该 channel 的读操作都会立即返回零值，并且第二个返回值为 false。
		//在 select 语句中，只要 channel 被关闭，对应的 case 就会立即就绪（可执行）！
		close(c.closeCh)
	}
}

// GetWithExpiration 获取缓存项及其剩余过期时间
func (c *lruCache) GetWithExpiration(key string) (Value, time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	//先查存在否
	elem, ok := c.items[key]
	if !ok {
		//缓存未命中
		return nil, 0, false
	}

	// 再检查是否过期
	now := time.Now()
	if expTime, hasExp := c.expires[key]; hasExp {
		if now.After(expTime) {
			// 已过期
			return nil, 0, false
		}

		// 计算剩余过期时间
		ttl := expTime.Sub(now)
		c.list.MoveToBack(elem)
		return elem.Value.(*lruEntry).value, ttl, true
	}

	// 如果无过期时间
	//直接给他移到链表尾部
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, 0, true
}

// GetExpiration 仅获取过期时间，不要获取值
// bool表示是否设置了过期时间
func (c *lruCache) GetExpiration(key string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expTime, ok := c.expires[key]
	return expTime, ok
}

// UpdateExpiration 更新过期时间
func (c *lruCache) UpdateExpiration(key string, expiration time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	//先查存在性
	if _, ok := c.items[key]; !ok {
		return false
	}
	//更新过期时间
	if expiration > 0 {
		c.expires[key] = time.Now().Add(expiration)
	} else {
		delete(c.expires, key) //标记为永不过期
	}

	return true
}

// UsedBytes 返回当前使用的字节数
func (c *lruCache) UsedBytes() int64 {
	c.mu.RLock() //都加读锁
	defer c.mu.RUnlock()
	return c.usedBytes
}

// MaxBytes 返回最大允许字节数
func (c *lruCache) MaxBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

// SetMaxBytes 设置最大允许字节数并触发淘汰
func (c *lruCache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	//设置新的最大允许内存
	c.maxBytes = maxBytes
	if maxBytes > 0 {
		//清理超内存项目
		c.evict()
	}
}
