package define

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rentiansheng/incenses/src/context"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

type MetricTaskImpl interface {
	Get(ctx context.Context) ([]MetricTask, error)
	TaskDone(ctx context.Context, name string, nextCycleTime, lastFinishTime uint64) error
	Add(ctx context.Context, info MetricTask, extra map[string]interface{}) error
	ModifyOutputIndexName(ctx context.Context, taskName, indexName string) error
	// List(ctx context.Context, page Page)([]MetricTask, error)
	// ChangeStatus(ctx context.Context, name string, from, to StatusEnumType, user string) error
}

type MetricTask struct {
	// 任务的名字，同时也是指标名字
	TaskName string `json:"task_name"  gorm:"column:task_name"`
	// 指标周期（1年，2季，3月，4周，5日，6时）（接口必须）
	TaskCycle TaskCycleType `json:"task_cycle" gorm:"column:task_cycle"`
	// 周期执行方式，1 周期结束后执行，2周期中每天计算一次
	CycleMode CycleModeType `json:"cycle_mode" gorm:"column:cycle_mode"`
	// 向前计算的周期数
	CalculateCycle uint8 `json:"calculate_cycle" gorm:"column:calculate_cycle"`
	// 任务状态， 1 正常，可以允许， 2. 暂停，不被执行 3. 待删除
	TaskStatus StatusEnumType `json:"task_status" gorm:"column:task_status"`

	// 如何处理start 开始处理任务的时间， 有start+cycle 可以选出结束时间
	TaskStart   uint64                              `json:"task_start" gorm:"column:task_start"`
	Collect     MetricTaskPluginCollectConfig       `json:"collect" gorm:"column:collect"`
	Filters     MetricTaskPluginConfigArr           `json:"filters" gorm:"column:filters"`
	Aggregators MetricTaskPluginAggregatorConfigArr `json:"aggregators" gorm:"column:aggregators"`
	Output      MetricTaskPluginOutputConfig        `json:"output" gorm:"column:output"`
	//Power          MetricPower                         `json:"power" gorm:"column:power"`
	LastFinishTime uint64 `json:"last_finish_time" gorm:"column:last_finish_time"`

	OutputIndexName string `json:"output_index_name" gorm:"column:output_index_name"`
}

type TaskCycleType uint8

const (
	TaskCycleTypeYear = iota + 1
	TaskCycleTypeQuarter
	TaskCycleTypeMonthly
	TaskCycleTypeWeekly
	TaskCycleTypeDay
	TaskCycleTypeHour
)

func (m MetricTask) Map() map[string]interface{} {
	return map[string]interface{}{
		"task_name":         m.TaskName,
		"task_cycle":        m.TaskCycle,
		"cycle_mode":        m.CycleMode,
		"calculate_cycle":   m.CalculateCycle,
		"task_status":       m.TaskStatus,
		"task_start":        m.TaskStart,
		"collect":           m.Collect,
		"filters":           m.Filters,
		"aggregators":       m.Aggregators,
		"output":            m.Output,
		"last_finish_time":  m.LastFinishTime,
		"output_index_name": m.OutputIndexName,
	}
}

type StatusEnumType int8

const (
	// StatusEnumTypeNormal 正在执行的任务
	StatusEnumTypeNormal StatusEnumType = 1
	// StatusEnumTypePaused 已经暂停的任务
	StatusEnumTypePaused StatusEnumType = 2
	// StatusEnumTypeDelete 需要清理的任务
	StatusEnumTypeDelete StatusEnumType = 3
)

type MetricTaskPluginConfig struct {
	Name   string    `json:"name" gorm:"column:name"`
	Config RAWConfig `json:"config" gorm:"column:config"`
}

// MetricTaskPluginConfigArr gorm 对json类型反序列有问题， 需要在字段的维度实现Scan 和 Value
type MetricTaskPluginConfigArr []MetricTaskPluginConfig

// Scan scan value into Jsonb, implements sql.Scanner interface
func (m *MetricTaskPluginConfigArr) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	err := json.Unmarshal(bytes, m)
	return err
}

// Value return json value, implement driver.Valuer interface
func (m MetricTaskPluginConfigArr) Value() (driver.Value, error) {
	return json.Marshal(m)
}

type MetricTaskPluginAggregatorConfig struct {
	Name   string    `json:"name" gorm:"column:name"`
	Config RAWConfig `json:"config" gorm:"column:config"`
	// 暂未启用
	Next *MetricTaskPluginConfig `json:"next" gorm:"column:next"`
}

// MetricTaskPluginAggregatorConfigArr gorm 对json类型反序列有问题， 需要在字段的维度实现Scan 和 Value
type MetricTaskPluginAggregatorConfigArr []MetricTaskPluginAggregatorConfig

// Scan scan value into Jsonb, implements sql.Scanner interface
func (m *MetricTaskPluginAggregatorConfigArr) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	err := json.Unmarshal(bytes, m)
	return err
}

// Value return json value, implement driver.Valuer interface
func (m MetricTaskPluginAggregatorConfigArr) Value() (driver.Value, error) {
	return json.Marshal(m)
}

type MetricTaskPluginCollectConfig struct {
	Name   string    `json:"name" gorm:"column:name"`
	Config RAWConfig `json:"config" gorm:"column:config"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (c *MetricTaskPluginCollectConfig) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	err := json.Unmarshal(bytes, c)
	return err
}

// Value return json value, implement driver.Valuer interface
func (c MetricTaskPluginCollectConfig) Value() (driver.Value, error) {
	return json.Marshal(c)
}

type RAWConfig []byte

func (d RAWConfig) MarshalJSON() ([]byte, error) {
	if string(d) == "" {
		return []byte(""), nil

	}
	ret := ([]byte)(d)
	return ret, nil
}

func (d *RAWConfig) UnmarshalJSON(data []byte) error {
	*d = (RAWConfig)(data)
	return nil
}

type MetricTaskPluginOutputConfig struct {
	Name string `json:"name" gorm:"column:name"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (m *MetricTaskPluginOutputConfig) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	err := json.Unmarshal(bytes, m)
	return err
}

// Value return json value, implement driver.Valuer interface
func (m MetricTaskPluginOutputConfig) Value() (driver.Value, error) {
	return json.Marshal(m)
}

type MetricPower map[string]int

// Scan scan value into Jsonb, implements sql.Scanner interface
func (m *MetricPower) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal metric power value:", value))
	}

	err := json.Unmarshal(bytes, m)
	return err
}

// Value return json value, implement driver.Valuer interface
func (m MetricPower) Value() (driver.Value, error) {
	return json.Marshal(m)
}
