package singleflight

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoDuplicateExecutionWithSameKey(t *testing.T) {
	const goroutines = 1000
	const rounds = 100

	for round := 0; round < rounds; round++ {
		var g Group       //每次循环创建一个Group
		var fnCalls int64 //fn执行次数

		start := make(chan struct{})     // 让所有goroutine同时跑起来
		fnStarted := make(chan struct{}) //等至少1个fn已经开始执行
		releaseFn := make(chan struct{}) // 卡主fn，不让其结束太快
		var once sync.Once

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()

				<-start
				_, err := g.Do("same-key", func() (interface{}, error) {
					//统计fn执行次数
					atomic.AddInt64(&fnCalls, 1)
					// 强制只能执行一次这段代码，哪怕是多个goroutine
					once.Do(func() {
						close(fnStarted)
					})
					<-releaseFn
					return "value", nil
				})
				if err != nil {
					t.Errorf("DO return error :%v", err)
				}
			}()
		}

		close(start)
		<-fnStarted
		time.Sleep(10 * time.Millisecond)
		close(releaseFn)
		wg.Wait()
		calls := atomic.LoadInt64(&fnCalls)
		if calls != 1 {
			t.Errorf("fn called %d times,want 1", calls)
		}
	}
}
