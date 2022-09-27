package times

import "time"

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

func CurDayStartTimeStamp() int64 {
	t := time.Now()
	newTime := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	return newTime.Unix()
}

func TimeStampToDate(ts int64) string {
	t := time.Unix(ts, 0)
	return t.Format("2006-01-02")
}
