package time_cycle

import (
	"fmt"
	"sort"
	"time"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

func GetTimeInterval(timestamp uint64, cycle CycleType) (TimeInterval, error) {
	return getTimeInterval(timestamp, cycle, timeStampTypeSecond)

}

func GetTimeIntervalMill(timestamp uint64, cycle CycleType) (TimeInterval, error) {
	return getTimeInterval(timestamp, cycle, timeStampTypeMillisecond)
}

// getTimeInterval 输入时间戳，输出时间戳所在周期的开始时间和结束时间
func getTimeInterval(timestamp uint64, cycle CycleType, timeStampType timeStampType) (TimeInterval, error) {
	var res TimeInterval
	resList, err := getTimeIntervalList([]uint64{timestamp}, cycle, timeStampType, true, true)
	if err != nil {
		return res, err
	}
	if len(resList) < 1 {
		return res, fmt.Errorf("unexpected err: result list of getTimeIntervalList is empty. "+
			"timestamp: %v, cycle: %v, timestamp type: %v", timestamp, cycle, timeStampType)
	}
	return resList[0], nil
}

// GetTimeIntervalList 输入时间戳列表以及周期类型，输出对应周期的起始时间及结束时间
// timeStampType 输入及输出时间戳的类型, 不输入则默认为秒级时间戳
// needSort 输出的周期是否排序
// removeDuplicate 是否去除重复的周期
func getTimeIntervalList(timeList []uint64, cycle CycleType, timeStampType timeStampType, needSort, removeDuplicate bool) ([]TimeInterval, error) {
	if timeStampType == timeStampTypeMillisecond { // 转化为秒级时间戳处理
		for i := range timeList {
			timeList[i] = timeList[i] / thousand
		}
	}

	handler, ok := intervalHandlerMap[cycle]
	if !ok {
		return nil, fmt.Errorf("Unknown cycle type, support year(1), quarter(2), month(3), week(4), day(5). ")
	}
	return getIntervalList(timeList, handler, timeStampType, needSort, removeDuplicate)
}

func getIntervalList(timeList []uint64, handler IntervalHandler, timeStampType timeStampType, needSort, removeDuplicate bool) ([]TimeInterval, error) {
	res := make([]TimeInterval, 0, len(timeList))
	for _, timeStamp := range timeList {
		interval, err := handler(timeStamp)
		if err != nil {
			return nil, err
		}
		res = append(res, interval)
	}
	return sortTimeInterval(res, timeStampType, needSort, removeDuplicate), nil
}

// sortTimeInterval 处理排序及去重
func sortTimeInterval(tiList []TimeInterval, timeStampType timeStampType, needSort, removeDuplicate bool) []TimeInterval {
	if len(tiList) == 0 {
		return tiList
	}
	// 排序
	if needSort {
		sort.Slice(tiList, func(i, j int) bool {
			if tiList[i].Begin < tiList[j].Begin {
				return true
			}
			return false
		})
	}

	// 去重
	if removeDuplicate {
		temp := make([]TimeInterval, 0, len(tiList))
		resSet := make(map[uint64]struct{}, len(tiList))
		for _, ti := range tiList {
			_, ok := resSet[ti.Begin]
			if !ok {
				temp = append(temp, ti)
				resSet[ti.Begin] = struct{}{}
			}
		}
		tiList = temp
	}

	if timeStampType == timeStampTypeMillisecond {
		for i := range tiList {
			tiList[i].Begin = thousand * tiList[i].Begin
			tiList[i].End = thousand*tiList[i].End + endTimeFromSecondToMillisecond
		}
	}
	return tiList
}

func getYearInterval(ts uint64) (TimeInterval, error) {
	timeInput := time.Unix(int64(ts), 0)
	year := timeInput.Year()
	begin := time.Date(year, time.Month(1), 1, 0, 0, 0, 0, time.Now().Location()).Unix()
	end := time.Date(year+1, time.Month(1), 1, 0, 0, 0, 0, time.Now().Location()).Unix() - 1
	return TimeInterval{
		Begin: uint64(begin),
		End:   uint64(end),
	}, nil
}

func getQuarterInterval(ts uint64) (TimeInterval, error) {
	timeInput := time.Unix(int64(ts), 0)
	year := timeInput.Year()
	month := timeInput.Month()
	var quarter int
	switch {
	case month <= endMonthOfQuarterOne:
		quarter = quarterOne
	case month > endMonthOfQuarterOne && month <= endMonthOfQuarterTwo:
		quarter = quarterTwo
	case month > endMonthOfQuarterTwo && month <= endMonthOfQuarterThree:
		quarter = quarterThree
	default:
		quarter = quarterFour
	}
	return quarterIntervalByYearAndQ(year, quarter)
}

func quarterIntervalByYearAndQ(year, quarter int) (TimeInterval, error) {
	var res TimeInterval
	if year < 0 {
		return res, fmt.Errorf("invalid year: %v", year)
	}
	if quarter < 1 || quarter > 4 {
		return res, fmt.Errorf("invalid quarter: %v", year)
	}
	// 季度开始的时间
	beginMonth := quarter*3 - 2
	beginTime := time.Date(year, time.Month(beginMonth), 1, 0, 0, 0, 0, time.Now().Location()).Unix()

	// 季度结束的时间
	endMonth := quarter * 3
	endTime := time.Date(year, time.Month(endMonth+1), 1, 0, 0, 0, 0, time.Now().Location()).Unix() - 1
	return TimeInterval{
		Begin: uint64(beginTime),
		End:   uint64(endTime),
	}, nil
}

func getMonthInterval(ts uint64) (TimeInterval, error) {
	timeInput := time.Unix(int64(ts), 0)
	year := timeInput.Year()
	month := timeInput.Month()

	// 月开始的时间
	beginTime := time.Date(year, month, 1, 0, 0, 0, 0, time.Now().Location()).Unix()

	// 月结束的时间
	endTime := time.Date(year, month+1, 1, 0, 0, 0, 0, time.Now().Location()).Unix() - 1
	return TimeInterval{
		Begin: uint64(beginTime),
		End:   uint64(endTime),
	}, nil
}

func getDayInterval(ts uint64) (TimeInterval, error) {
	timeInput := time.Unix(int64(ts), 0)
	year := timeInput.Year()
	month := timeInput.Month()
	day := timeInput.Day()

	// 天开始时间
	beginTime := time.Date(year, month, day, 0, 0, 0, 0, time.Now().Location()).Unix()

	// 天结束时间
	endTime := time.Date(year, month, day+1, 0, 0, 0, 0, time.Now().Location()).Unix() - 1
	return TimeInterval{
		Begin: uint64(beginTime),
		End:   uint64(endTime),
	}, nil
}

func getHourInterval(ts uint64) (TimeInterval, error) {
	timeInput := time.Unix(int64(ts), 0)
	year := timeInput.Year()
	month := timeInput.Month()
	day := timeInput.Day()
	hour := timeInput.Hour()

	// 小时开始时间
	beginTime := time.Date(year, month, day, hour, 0, 0, 0, time.Now().Location()).Unix()

	// 小时结束时间
	endTime := time.Date(year, month, day, hour+1, 0, 0, 0, time.Now().Location()).Unix() - 1
	return TimeInterval{
		Begin: uint64(beginTime),
		End:   uint64(endTime),
	}, nil
}

func getNaturalWeekInterval(ts uint64) (TimeInterval, error) {
	timeInput := time.Unix(int64(ts), 0)
	year := timeInput.Year()
	month := timeInput.Month()
	day := timeInput.Day()

	// 找到上一个周一
	lastMonday := time.Date(year, month, day, 0, 0, 0, 0, time.Now().Location())
	weekDay := int(lastMonday.Weekday())
	dayToSub := weekDay - weekDayOfMonday
	if dayToSub < 0 {
		dayToSub += dayOfOneWeek
	}
	timeToSub := time.Duration(dayToSub*24) * time.Hour
	lastMonday = lastMonday.Add(-timeToSub)

	beginTime := lastMonday.Unix()
	endTime := beginTime + OneWeekTimeSecond - 1
	return TimeInterval{
		Begin: uint64(beginTime),
		End:   uint64(endTime),
	}, nil
}
