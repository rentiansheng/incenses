package retry

import (
	"fmt"
	"testing"
	"time"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

func TestRetry0(t *testing.T) {
	cnt := 0
	retryNum := 0
	err := Retry(retryNum, func(idx int) (next bool, err error) {
		cnt = idx + 1
		return false, nil
	}, 0)
	if err != nil {
		t.Errorf("retry return error. err: %s", err.Error())
		return
	}
	if cnt != 1 {
		t.Errorf("execute count error. actual: %d, expect: %d", cnt, 1)
		return
	}
}

func TestRetry3Err(t *testing.T) {
	cnt := 0
	retryNum := 3
	err := Retry(retryNum, func(idx int) (next bool, err error) {
		cnt = idx + 1
		return true, fmt.Errorf("test error. idx: %d", idx)
	}, 0)
	if err == nil {
		t.Errorf("retry return error. actual: nil, expect: %s", err.Error())
		return
	}
	if cnt != retryNum {
		t.Errorf("execute count error. actual: %d, expect: %d", cnt, retryNum)
		return
	}
}

func TestRetryDelay(t *testing.T) {
	cnt := 0
	retryNum := 3
	startTime := time.Now()
	delayMill := time.Millisecond * 200
	err := Retry(retryNum, func(idx int) (next bool, err error) {
		cnt = idx + 1
		time.Sleep(time.Millisecond * 10)
		return true, fmt.Errorf("test error. idx: %d", idx)
	}, delayMill)
	if err == nil {
		t.Errorf("retry return error. actual: nil, expect: %s", err.Error())
		return
	}
	if cnt != retryNum {
		t.Errorf("execute count error. actual: %d, expect: %d", cnt, retryNum)
		return
	}
	expendMill := time.Now().Sub(startTime).Milliseconds()
	expectMill := int64(delayMill * time.Duration(retryNum) / time.Millisecond)
	if expendMill < expectMill {
		t.Errorf("execute count error. actual: %d, expect: >%d", expendMill, expectMill)
		return
	}

}

func TestRetry10Success(t *testing.T) {
	cnt := 0
	retryNum := 3
	err := Retry(retryNum, func(idx int) (next bool, err error) {
		cnt = idx + 1
		return false, fmt.Errorf("test error. idx: %d", idx)
	}, time.Millisecond*200)
	if err == nil {
		t.Errorf("retry return error. actual: nil, expect: %s", err.Error())
		return
	}
	if cnt != 1 {
		t.Errorf("execute count error. actual: %d, expect: %d", cnt, 1)
		return
	}
}
