package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// LRU2的作用是防止缓存污染，比如一次性来了一千个冷门数据，
// 但是都只访问了一次，就把所有的热数据寄出去了导致命中率暴跌。
// LRU2就是要访问1次的数据，就放入一级缓存，访问一次后，就放入二级缓存。
// 只有热数据会经过二级缓存，不会污染缓存。
type lru2Store struct {
	locks       []sync.Mutex                  //把缓存分成多个桶，每个桶有自己的锁，不同的桶可以并行
	caches      [][2]*cache                   //两级缓存，i表示第i个桶，caches[i][0]=L1，caches[i][1]=L2
	onEvicted   func(key string, value Value) //回调函数，来自store.Options
	cleanupTick *time.Ticker                  //定时清理
	mask        int32                         //哈希掩码，用来计算key落在哪个桶
}

// 内部缓存核心实现，包含双向链表和节点存储
// m是存数据的比如，m[0] = node，m[1] = node，node里面有key和value和过期时间
// 每一个桶的每缓存都是独立的。
type cache struct {
	// 	重要！！！！
	//  dlnk = [
	//     // [0] 是哨兵节点 (sentinel)
	//     [2, 1], // dlnk[0][0]=2 (尾部是索引2的节点), dlnk[0][1]=1 (头部是索引1的节点)
	//     // [1] 是第一个真实节点 (最近被访问的，在链表头部)
	//     [0, 2], // dlnk[1][0]=0 (前驱是哨兵), dlnk[1][1]=2 (后继是索引2的节点)
	//     // [2] 是第二个真实节点 (最久未被访问的，在链表尾部)
	//     [1, 0]  // dlnk[2][0]=1 (前驱是索引1的节点), dlnk[2][1]=0 (后继是哨兵)

	// dlnk[0]是哨兵节点，记录链表头尾，dlnk[0][0]是哨兵节点的前驱节点，dlnk[0][1]是哨兵节点的后继节点
	//dlnk[i][0]是节点i的前驱节点，dlnk[i][1]是节点i的后继节点
	dlnk [][2]uint16       // 双向链表，0 表示前驱，1 表示后继，dlnk的索引和m一样
	m    []node            // 预分配内存存储节点，例如 m = make([]node, capacity)，后续不在产生新节点,复用减少gc压力
	hmap map[string]uint16 // 键到节点索引的映射，例如"user1" → 3
	last uint16            // 最后一个节点元素的索引
}

func newLRU2Cache(opts Options) *lru2Store {
	if opts.BucketCount == 0 {
		opts.BucketCount = 16 //桶数量默认16个
	}
	if opts.CapPerBucket == 0 {
		opts.CapPerBucket = 1024 //每个桶L1的容量
	}
	if opts.Level2Cap == 0 {
		opts.Level2Cap = 1024 //每个桶L2的容量
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute //清理的间隔时间
	}
	//计算掩码，用来计算key落在哪个桶
	mask := maskOfNextPowOf2(opts.BucketCount)
	//初始化lru2Store实例
	s := &lru2Store{
		locks:       make([]sync.Mutex, mask+1), //mask+1是桶的数量，让切片长度等于桶数量
		caches:      make([][2]*cache, mask+1),
		onEvicted:   opts.OnEvicted,
		cleanupTick: time.NewTicker(opts.CleanupInterval),
		mask:        int32(mask),
	}

	for i := range s.caches {
		s.caches[i][0] = Create(opts.CapPerBucket) //L1
		s.caches[i][1] = Create(opts.Level2Cap)    //L2
	}

	if opts.CleanupInterval > 0 {
		go s.cleanupLoop() //启动后台携程，定期清理缓存
	}

	return s
}

// 实现了 BKDR 哈希算法，用于计算键的哈希值
func hashBKRD(s string) (hash int32) {
	for i := 0; i < len(s); i++ {
		hash = hash*131 + int32(s[i])
	}

	return hash
}

// 从缓存中删除键对应的项
func (c *cache) del(key string) (*node, int, int64) {
	//c.hmap先找到index，再用m通过index找到node，且没有过期
	if idx, ok := c.hmap[key]; ok && c.m[idx-1].expireAt > 0 {
		e := c.m[idx-1].expireAt //拿到过期时间
		c.m[idx-1].expireAt = 0  // 标记为已删除，并没有真的删除，只是设置为过期，然后后面不会用到
		//var clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)
		c.adjust(idx, n, p)      // 移动到链表尾部
		return &c.m[idx-1], 1, e //1 表示找到了
	}

	return nil, 0, 0
}

func (s *lru2Store) Get(key string) (Value, bool) {
	//先把key转换为哈希值，然后位运算得到桶的索引
	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock() //对桶加锁
	defer s.locks[idx].Unlock()

	currentTime := Now()

	// 首先检查一级缓存
	//在idx桶的一级缓存L1中，尝试删除并返回key对应的缓存项
	n1, status1, expireAt := s.caches[idx][0].del(key)
	if status1 > 0 { //status>0表示找到，就是第二次访问了
		// 从一级缓存找到项目
		if expireAt > 0 && currentTime >= expireAt {
			// 项目已过期，删除它
			s.delete(key, idx)
			fmt.Println("找到项目已过期，删除它")
			return nil, false
		}

		// 项目有效，将其移至二级缓存，put是向缓存中添加项
		s.caches[idx][1].put(key, n1.v, expireAt, s.onEvicted)
		fmt.Println("项目有效，将其移至二级缓存")
		return n1.v, true
	}

	// 一级缓存未找到，检查二级缓存
	//_get的作用是从二级缓存中获取key对应的缓存项
	n2, status2 := s._get(key, idx, 1)
	if status2 > 0 && n2 != nil {
		if n2.expireAt > 0 && currentTime >= n2.expireAt {
			// 项目已过期，删除它
			s.delete(key, idx)
			fmt.Println("找到项目已过期，删除它")
			return nil, false
		}

		return n2.v, true
	}

	return nil, false
}

// Set 添加或更新缓存项
func (s *lru2Store) Set(key string, value Value) error {
	return s.SetWithExpiration(key, value, 9999999999999999)
}

// SetWithExpiration 添加或更新缓存项
func (s *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	// 计算过期时间 - 确保单位一致
	expireAt := int64(0)
	if expiration > 0 {
		// now() 返回纳秒时间戳，确保 expiration 也是纳秒单位
		// expireAt表示绝对过期时间，expiration是相对比如5s
		expireAt = Now() + int64(expiration.Nanoseconds())
	}
	//计算桶的索引
	idx := hashBKRD(key) & s.mask
	//加锁
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	// 放入一级缓存
	s.caches[idx][0].put(key, value, expireAt, s.onEvicted)

	return nil
}

// Delete 实现Store接口
// Delete解决的是对外接口
// delete处理store级的逻辑，里面可以有两个cache，比如L1.cache和L2.cache
// del处理的是单独的cache的逻辑
func (s *lru2Store) Delete(key string) bool {
	idx := hashBKRD(key) & s.mask //拿到桶id
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock() //计算桶号并加减锁

	return s.delete(key, idx)
}

// Clear 实现Store接口
// 清空缓存
func (s *lru2Store) Clear() {
	var keys []string
	//遍历所有的caches
	for i := range s.caches {
		s.locks[i].Lock()
		//收集L1的所有key
		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			keys = append(keys, key)
			return true
		})
		//收集L2的所有key
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			// 检查键是否已经收集（避免重复）
			for _, k := range keys {
				if key == k {
					return true
				}
			}
			keys = append(keys, key)
			return true
		})

		s.locks[i].Unlock()
	}
	//删除所有收集到的key
	for _, key := range keys {
		s.Delete(key)
	}

	//s.expirations = sync.Map{}
}

// Len 实现Store接口
// Len 返回缓存中的项数，统计L2 L2所有有效的key的数量
func (s *lru2Store) Len() int {
	count := 0
	//caches是存放二级缓存的切片
	for i := range s.caches {
		s.locks[i].Lock()
		//walk用于遍历缓存中所有的有效项，这里是遍历L1
		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})
		//遍历L2
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})

		s.locks[i].Unlock()
	}

	return count
}

// Close 关闭缓存相关资源，停止周期性清理携程
func (s *lru2Store) Close() {
	if s.cleanupTick != nil {
		s.cleanupTick.Stop() // 停止计时器，让后面的cleanupLoop不再执行
	}
}

// 内部时钟，减少 time.Now() 调用造成的 GC 压力
var clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)

// 返回 clock 变量的当前值。
// atomic.LoadInt64 是原子操作，用于保证在多线程/协程环境中安全地读取 clock 变量的值
func Now() int64 { return atomic.LoadInt64(&clock) }

func init() {
	go func() {
		for {
			//原子写操作，写入不可被打断。
			atomic.StoreInt64(&clock, time.Now().UnixNano()) // 每秒校准一次
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				//原子加法操作，写入不可被打断。
				atomic.AddInt64(&clock, int64(100*time.Millisecond)) // 保持 clock 在一个精确的时间范围内，同时避免频繁的系统调用
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// maskOfNextPowOf2 计算大于或等于输入值的最近 2 的幂次方减一作为掩码值
func maskOfNextPowOf2(cap uint16) uint16 {
	if cap > 0 && cap&(cap-1) == 0 {
		//cap&(cap-1)==0 判断是否是2的幂
		//因为2的幂在二进制中只有一个1，其他全是0
		//最后用hash&(cap-1)求得桶索引
		return cap - 1 //也就是mask
	}

	// 通过多次右移和按位或操作，将二进制中最高的 1 位右边的所有位都填充为 1
	// |= 的意思是按位或赋值 a |= b 等价于 a = a | b。
	// >>表示将一个数的二进制表示向右移动指定的位数。 相当于除以 2 的 N 次方
	// |表示取 "或" 操作，有1个是1就是1，只有两个0才是0
	cap |= cap >> 1 //等价于cap = cap | (cap >> 1)
	cap |= cap >> 2
	cap |= cap >> 4

	return cap | (cap >> 8)
}

type node struct {
	k        string
	v        Value
	expireAt int64 // 过期时间戳，expireAt = 0 表示已删除
}

func Create(cap uint16) *cache {
	return &cache{
		dlnk: make([][2]uint16, cap+1), //+1是因为有哨兵节点
		m:    make([]node, cap),        //提前分配，不再做动态扩容
		hmap: make(map[string]uint16, cap),
		last: 0,
	}
}

// 向缓存中添加项，如果是新增返回 1，更新返回 0
// 只作用于单独的cahce层，不控制L1,L2的升降，只是控制单独的cache的缓存，移动
func (c *cache) put(key string, val Value, expireAt int64, onEvicted func(string, Value)) int {
	// 如果缓存存在的话，先拿到节点索引idx
	if idx, ok := c.hmap[key]; ok {
		//从node中拿到val和过期时间
		c.m[idx-1].v, c.m[idx-1].expireAt = val, expireAt
		c.adjust(idx, p, n) // 刷新到链表头部，这里p=0,n=1
		return 0            // 表示更新
	}

	//如果key不存在且缓存满了。就淘汰尾部，复用节点。
	//cap(c.m)是缓存的容量，如果缓存用完了
	if c.last == uint16(cap(c.m)) {
		//淘汰尾部
		tail := &c.m[c.dlnk[0][p]-1]
		if onEvicted != nil && (*tail).expireAt > 0 {
			//回调函数，比如释放资源，写日志等等
			onEvicted((*tail).k, (*tail).v)
		}
		//删除旧key
		delete(c.hmap, (*tail).k)
		//节点复用，不新new一个node，直接覆盖tail
		c.hmap[key], (*tail).k, (*tail).v, (*tail).expireAt = c.dlnk[0][p], key, val, expireAt
		c.adjust(c.dlnk[0][p], p, n) // 把tail刷新到链表头部

		return 1
	}
	//如果缓存没满，先新增节点，last表示最后一个节点的索引
	c.last++
	if len(c.hmap) <= 0 { //如果是第一个节点
		c.dlnk[0][p] = c.last //更新tail节点
	} else {
		c.dlnk[c.dlnk[0][n]][p] = c.last //新节点插入头部，最后插入的放在最前面
	}

	// 初始化新节点并更新链表指针
	c.m[c.last-1].k = key
	c.m[c.last-1].v = val
	c.m[c.last-1].expireAt = expireAt
	c.dlnk[c.last] = [2]uint16{0, c.dlnk[0][n]} //把last插入头部
	c.hmap[key] = c.last
	c.dlnk[0][n] = c.last

	return 1
}

// 从缓存中获取键对应的节点和状态
func (c *cache) get(key string) (*node, int) {
	if idx, ok := c.hmap[key]; ok {
		c.adjust(idx, p, n)
		return &c.m[idx-1], 1
	}
	return nil, 0
}

// 遍历缓存中的所有有效项
func (c *cache) walk(walker func(key string, value Value, expireAt int64) bool) {
	// 从头部开始，一直往下找，直到找到0，也就是尾部
	for idx := c.dlnk[0][n]; idx != 0; idx = c.dlnk[idx][n] {
		//用walk函数来遍历是为了把过期时间这条件加进去，遍历原数组不能实现这个筛选的功能
		// 如果节点没有过期，就调用传进来的回调函数walker
		if c.m[idx-1].expireAt > 0 && !walker(c.m[idx-1].k, c.m[idx-1].v, c.m[idx-1].expireAt) {
			return
		}
	}
}

// 调整节点在链表中的位置
// adjust(idx, f, t)表示：0,1代表移到表头，1,0移到尾
// 下面的注释假设是0,1
// f代表前面的指针，t代表后面的指针
func (c *cache) adjust(idx, f, t uint16) {
	if c.dlnk[idx][f] != 0 { //先判断idx是否有前后节点
		//假设HEAD <-> A <-> idx <-> B
		// dlnk[idx][f] = A
		// dlnk[idx][t] = B
		// 执行下面两行后。
		// 则dlnk[B][f] = A，等价于B.prev = A
		//dlnk[A][t] = B，等价于A.next = B，就把idx摘出来了
		c.dlnk[c.dlnk[idx][t]][f] = c.dlnk[idx][f]
		c.dlnk[c.dlnk[idx][f]][t] = c.dlnk[idx][t]
		//清空旧关系，AB没有和idx的关系了，但是idx和head还保留着原来的关系，要修改
		c.dlnk[idx][f] = 0            //等价于idx.prev = HEAD
		c.dlnk[idx][t] = c.dlnk[0][t] //等价于idx.next = head.next,本例子中就是A节点
		c.dlnk[c.dlnk[0][t]][f] = idx //旧指针头指向idx，A.prev = idx
		c.dlnk[0][t] = idx            //head.next = idx
	}
}

// level=1代表L2缓存
func (s *lru2Store) _get(key string, idx, level int32) (*node, int) {
	//n,st代表节点和状态，st>0表示命中缓存，n！=nil表示存在
	if n, st := s.caches[idx][level].get(key); st > 0 && n != nil {
		currentTime := Now()
		if n.expireAt <= 0 || currentTime >= n.expireAt {
			// 过期或已删除
			return nil, 0
		}
		return n, st
	}

	return nil, 0
}

func (s *lru2Store) delete(key string, idx int32) bool {
	//n1,s1,_对应被删除节点，是否找到，过期时间
	n1, s1, _ := s.caches[idx][0].del(key) //去L1尝试删除缓存
	n2, s2, _ := s.caches[idx][1].del(key) //去L2尝试删除缓存
	deleted := s1 > 0 || s2 > 0            //找到一个就行

	if deleted && s.onEvicted != nil {
		if n1 != nil && n1.v != nil {
			s.onEvicted(key, n1.v)
		} else if n2 != nil && n2.v != nil {
			s.onEvicted(key, n2.v)
		}
	}

	if deleted {
		// s.expirations.Delete(key)
	}

	return deleted
}

// 定时删除过期的节点
func (s *lru2Store) cleanupLoop() {
	for range s.cleanupTick.C { //定时触发
		currentTime := Now()

		for i := range s.caches {
			s.locks[i].Lock()

			// 检查并清理过期项目
			var expiredKeys []string
			//收集过期的L1的key
			s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
				if expireAt > 0 && currentTime >= expireAt {
					expiredKeys = append(expiredKeys, key)
				}
				return true
			})
			//收集过期的L2的key
			s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
				if expireAt > 0 && currentTime >= expireAt {
					for _, k := range expiredKeys {
						if key == k {
							// 避免重复
							return true
						}
					}
					expiredKeys = append(expiredKeys, key)
				}
				return true
			})

			for _, key := range expiredKeys {
				s.delete(key, int32(i))
			}

			s.locks[i].Unlock()
		}
	}
}
