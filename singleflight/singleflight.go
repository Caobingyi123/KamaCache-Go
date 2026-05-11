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
	// m里存的是key -> *call
	m sync.Map // 使用sync.Map来优化并发性能,减少锁的使用，多用于多读少写。
}

// Do 针对相同的key，保证多次调用Do()，都只会调用一次fn
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {

	c := &call{}
	c.wg.Add(1)

	// LoadOrStore 原子操作
	// 如果 key 已经存在，返回已有的 call，loaded=true
	// 如果 key 不存在，把当前 c 存进去，loaded=false
	actual, loaded := g.m.LoadOrStore(key, c)
	if loaded {
		existing := actual.(*call)
		existing.wg.Wait()
		return existing.val, existing.err
	}

	//用defer保证程序就算挂掉也能Done和Delete
	defer func() {
		c.wg.Done()     //唤醒所有的请求
		g.m.Delete(key) //清理缓存，防止内存泄露，本次的key已经处理完了所以删掉
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
