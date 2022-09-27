package define

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

type Record interface {
	Data() map[string]string
	Update(key, value string) error
	Field() map[string]float64
	UUID() string
}

type record struct {
	// change to lable
	data  map[string]string
	field map[string]float64

	// uuid  数据去重使用, 可能是多个字段聚合。
	uuid string
}

// NewRecord
//    Params:
//   	uuid: 数据去重使用, 可能是多个字段聚合。重复的数据回被过滤
func NewRecord(uuid string, data map[string]string, field map[string]float64) Record {
	return &record{
		data:  data,
		field: field,
		uuid:  uuid,
	}
}

func (r *record) Data() map[string]string {
	return r.data
}

func (r *record) Field() map[string]float64 {
	return r.Field()
}

func (r *record) Update(key, value string) error {
	r.data[key] = value
	return nil
}

func (r *record) UUID() string {
	return r.uuid
}

type InputParams struct {
	Method string   `json:"method"`
	Fields []string `json:"fields"`
	Start  int32    `json:"start"`
	End    int32    `json:"end"`
}

type OutputData struct {
	MetricData
}

// MetricData 计算产生的数据
type MetricData struct {
	MetricKey string
	Value     map[string]float64     `json:"value"`
	Extra     map[string]interface{} `json:"extra"`
}

type MetricMetadata struct {
	// 指标名字
	MetricName string        `json:"name"  `
	Start      uint64        `json:"start"`
	End        uint64        `json:"end"`
	Cycle      TaskCycleType `json:"cycle"`
	CycleMode  CycleModeType `json:"interval_mode"`
	// output 插件需要根据这个值，来确定数据是否需要更新
	LastFinishTime uint64 `json:"last_finish_time"`
}

// CycleModeType 周期执行方式，1 周期结束后执行，2周期中每天计算一次
type CycleModeType int8

const (
	// CycleModeTypeEnd 1 周期结束后执行
	CycleModeTypeEnd CycleModeType = iota + 1
	// CycleModeTypeInnerDay 2周期中每天计算一次
	CycleModeTypeInnerDay = 2
	CycleModeTypeAlways   = 10
)
