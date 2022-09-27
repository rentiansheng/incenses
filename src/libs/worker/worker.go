package worker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

type Worker interface {
	Run(ctx context.Context, f func(ctx context.Context) error)
	Wait() error
}

type worker struct {
	num            int
	exitSignalChn  chan struct{}
	cntChn         chan struct{}
	execErr        error
	exitSignalOnce *sync.Once
	waitAllExit    bool
}

func NewWorker(num int) Worker {
	w := &worker{
		num:            num,
		cntChn:         make(chan struct{}, num),
		exitSignalChn:  make(chan struct{}, 0),
		exitSignalOnce: &sync.Once{},
	}

	for idx := 0; idx < num; idx++ {
		w.cntChn <- struct{}{}
	}
	return w
}

func NewWaitExecWorker(num int) Worker {
	w := &worker{
		num:            num,
		cntChn:         make(chan struct{}, num),
		exitSignalChn:  make(chan struct{}, 0),
		exitSignalOnce: &sync.Once{},
		waitAllExit:    true,
	}

	for idx := 0; idx < num; idx++ {
		w.cntChn <- struct{}{}
	}
	return w
}

func (w *worker) Run(ctx context.Context, f func(ctx context.Context) error) {
	select {
	case <-w.cntChn:

	case <-w.exitSignalChn:
		return
	}
	go func() {
		defer func() {
			w.cntChn <- struct{}{}
			if f := recover(); f != nil {
				w.setExecErr(fmt.Errorf("panic error.worker. %v", f))

			}
		}()

		if err := f(ctx); err != nil {
			w.setExecErr(err)
			return
		}
	}()
}

func (w *worker) Wait() error {
	ticker := time.NewTicker(time.Millisecond * 300)
	// 判断任务是否执行完成
	for range ticker.C {
		select {
		case <-ticker.C:
			if len(w.cntChn) == w.num {
				return w.execErr
			}
		case <-w.exitSignalChn:
			return w.execErr
		}

	}

	return w.execErr

}

func (w *worker) setExecErr(err error) {

	w.exitSignalOnce.Do(func() {
		// 设置执行错误, 必须放到 close(w.exitSignalChn) 前，避免现收到退出信号，
		// 错误没有赋值，实际有错误，但是返回没有错
		w.execErr = err
		if !w.waitAllExit {
			// 通知其他人退出
			close(w.exitSignalChn)
		}
	})

}
