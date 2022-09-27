package worker

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

// TestWorker1vs10 十个worker 只有一个任务
func TestWorker1vs10(t *testing.T) {
	worker := NewWorker(10)
	execCnt := 0
	worker.Run(context.TODO(), func(ctx context.Context) error {
		execCnt = 1
		return nil
	})
	if err := worker.Wait(); err != nil {
		t.Errorf("worker execute error. err: %s", err)
		return
	}
	if execCnt != 1 {
		t.Errorf("worker not execute function")
	}
}

// TestWorker1vs10Err 十个worker 只有一个任务,并且出现错误
func TestWorker1vs10Err(t *testing.T) {
	worker := NewWorker(10)
	worker.Run(context.TODO(), func(ctx context.Context) error {
		return fmt.Errorf("test error")
	})
	if err := worker.Wait(); err == nil {
		t.Errorf("worker execute function error, not return error")
		return
	}
}

// TestWorker1vs10 十个worker 只有一个任务
func TestWorkerRand10(t *testing.T) {
	num := rand.Intn(1000) + 20
	worker := NewWorker(10)
	execChn := make(chan struct{}, num)

	for idx := 0; idx < num; idx++ {
		worker.Run(context.TODO(), func(ctx context.Context) error {

			execChn <- struct{}{}
			return nil
		})
	}
	if err := worker.Wait(); err != nil {
		t.Errorf("worker execute error. err: %s", err)
		return
	}

	if len(execChn) != num {
		t.Errorf("worker not execute function")
	}
}

// TestWorker1vs10Err 十个worker 只有一个任务,并且出现错误
func TestWorkerRandErr(t *testing.T) {
	num := rand.Intn(1000) + 20
	errIdx := rand.Intn(num - 5)
	worker := NewWorker(10)
	execChn := make(chan struct{}, num)
	for idx := 0; idx < num; idx++ {
		worker.Run(context.TODO(), func(ctx context.Context) error {

			execChn <- struct{}{}
			if len(execChn) >= errIdx {
				return fmt.Errorf("test error")
			}
			return nil
		})
	}

	if err := worker.Wait(); err == nil {
		t.Errorf("worker execute function error, not return error")
		return
	}
	if len(execChn) >= num {
		t.Errorf("worker not execute function")
	}
}
