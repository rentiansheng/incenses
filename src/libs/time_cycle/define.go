package time_cycle

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

func init() {

	intervalHandlerMap = map[CycleType]IntervalHandler{
		MetricCycleYear:        getYearInterval,
		MetricCycleQuarter:     getQuarterInterval,
		MetricCycleMonth:       getMonthInterval,
		MetricCycleDay:         getDayInterval,
		MetricCycleHour:        getHourInterval,
		MetricCycleNaturalWeek: getNaturalWeekInterval,
	}
}

type timeStampType string

const (
	timeStampTypeSecond      timeStampType = "second"
	timeStampTypeMillisecond timeStampType = "millisecond"
)

// 指标周期（1年，2季，3月，4周，5日，6时）
const (
	thousand          uint64 = 1000
	weekDayOfMonday          = 1
	weekDayOfThursday        = 4
	dayOfOneWeek             = 7

	endMonthOfQuarterOne   = 3
	endMonthOfQuarterTwo   = 6
	endMonthOfQuarterThree = 9

	quarterOne   = 1
	quarterTwo   = 2
	quarterThree = 3
	quarterFour  = 4

	endTimeFromSecondToMillisecond uint64 = 999

	OneDayTimeSecond  = int64(24 * 60 * 60)
	OneWeekTimeSecond = 7 * OneDayTimeSecond
)

type CycleType uint8

const (
	MetricCycleYear        CycleType = 1
	MetricCycleQuarter               = 2
	MetricCycleMonth                 = 3
	MetricCycleDay                   = 5
	MetricCycleHour                  = 6
	MetricCycleNaturalWeek           = 7 // 自然周，周一到周日
)

type TimeInterval struct {
	Begin uint64
	End   uint64
}

var intervalHandlerMap map[CycleType]IntervalHandler

type IntervalHandler func(ts uint64) (TimeInterval, error)
