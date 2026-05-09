package singleflight

import (
	"fmt"
	"sync"
)

// 代表正在进行或已结束的请求
// call这里代表最小执行单元//
// call 是同一个 key 的所有并发请求共享的对象：
// 第 1 个请求创建 call，开始执行任务
// 第 2-100 个请求直接拿到这个 call，等待 wg 信号
// 第 1 个请求做完后，把结果存入 val/err，唤醒所有人
// 所有人都拿到同一个结果，完美！
type call struct {
	wg  sync.WaitGroup //等待信号
	val interface{}    //结果返回值
	err error          //错误
}

// Group manages all kinds of calls
type Group struct {
	m sync.Map // 使用sync.Map来优化并发性能,减少锁的使用，多用于多读少写。
}

// Do 针对相同的key，保证多次调用Do()，都只会调用一次fn
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// g.m.Load 从map中加载key对应的value
	if existing, ok := g.m.Load(key); ok {
		c := existing.(*call) // 类型断言
		c.wg.Wait()           // Wait for the existing request to finish
		return c.val, c.err   // 如果有缓存，直接返回
	}

	// If no ongoing request, create a new one
	c := &call{}
	c.wg.Add(1)
	g.m.Store(key, c) // 把key存入sync。map

	//用defer保证程序就算挂掉也能Done和Delete
	defer func() {
		c.wg.Done()     //唤醒所有的请求
		g.m.Delete(key) //清理缓存，防止内存泄露
	}()

	//用recover捕获panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				//把panic转换为error
				if err, ok := r.(error); ok { //类型断言
					c.err = err
				} else {
					c.err = fmt.Errorf("%v", r)
				}
			}
		}()
		c.val, c.err = fn()
	}()

	return c.val, c.err
}
