package retry

import "time"

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

const minDelay = time.Millisecond * 100
const DefaultDelay = minDelay

//
type fn func(idx int) (next bool, err error)

// Retry 用来做重试，
//    Params:
//       retryNum: 最大重试次数
//       f: 执行的函数, 返回值next 表示是否需要继续执行，不会判断err
//       delay: 重试间隔时间， 最小 time.Millisecond * 100
func Retry(retryNum int, f fn, delay time.Duration) error {
	if retryNum < 1 {
		retryNum = 1
	}
	if delay < minDelay {
		delay = minDelay
	}

	next, err := bool(false), error(nil)
	for idx := 0; idx < retryNum; idx++ {
		next, err = f(idx)
		// 需要继续执行
		if next {
			time.Sleep(delay)
			continue
		}
		// 错误不需要继续执行
		if err != nil {
			return err

		}
		return nil
	}
	// 这里是为了保证，超过重试次数的时候，返回最后一次的错误
	return err
}

// DefaultRetry 默认retry， 最多重试三次，每次间隔100ms
func DefaultRetry(f fn) error {
	return Retry(3, f, DefaultDelay)
}
