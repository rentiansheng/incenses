package define

import (
	"github.com/rentiansheng/incenses/src/context"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

// Collect 收集数据插件
type Collect interface {
	// Name 插件的名字，必须全局唯一
	Name() string
	// Keys 用做计算的维度
	Keys(ctx context.Context) ([]string, error)
	// Run 执行
	Run(ctx context.Context, key string, start, end uint64, input chan Record) error

	// SetConfig 修改配置
	SetConfig(ctx context.Context, config []byte) error
	// Description 用来描述配置
	Description() string

	// GetHooks 获取注入插件，用来放到流程不同流程执行
	//GetHooks() Hooks
}

// Filter data transform
type Filter interface {
	// Name 插件的名字，必须全局唯一
	Name() string
	// Run 执行
	Run(ctx context.Context, key string, data Record) error
	// SetConfig 初始化的时候，用来放入配置
	SetConfig(ctx context.Context, config []byte) error
	// Description 用来描述配置
	Description() string
}

// Aggregator calculate metric
type Aggregator interface {
	// Name 插件的名字，必须全局唯一
	Name() string
	// Run 执行
	Run(ctx context.Context, key string, data Record) error
	// SetConfig 初始化的时候，用来放入配置
	SetConfig(ctx context.Context, config []byte) error
	// Description 用来描述配置
	Description() string
	// Metric 存储的时候，获取计算的值
	Metric(ctx context.Context) (string, float64)
	MetricExtra(ctx context.Context) (string, interface{}, bool)
	SetMetricMetadata(ctx context.Context, data MetricMetadata) error
}

// Output record to storage
type Output interface {
	// Name 插件的名字，必须全局唯一
	Name() string
	// Write 输出数据到目标
	Write(ctx context.Context, data OutputData) error
	// Exists 插件是否有相同的问题件， 这个时刻，数据还没有计算
	Exists(ctx context.Context, metricKey string) (bool, error)
	// Description 用来描述配置
	Description() string
	// IndexName string
	IndexName(ctx context.Context) (string, error)
	SetMetricMetadata(ctx context.Context, data MetricMetadata) error
}

type CollectInput struct {
	Plugin          Collect
	Fields          []string
	Labels          []string
	OutputPluginChn chan MetricData
	MetricMetadata  MetricMetadata
}

type FilterInput struct {
	Input             chan Record
	Output            chan Record
	Key               string
	Plugins           []Filter
	CancelKeyWorkerFn context.CancelFunc
}

type FilterPlugins struct {
	Name   string `json:"name"`
	Config []byte `json:"config"`
}

// AggregatorInput
// Plan: 如何支持多级， 上一级Aggregator带入到next aggregator 中
type AggregatorInput struct {
	Key string

	Input             chan Record
	Output            chan MetricData
	Plugins           []Aggregator
	CancelKeyWorkerFn context.CancelFunc
}

type AggregatorPlugins struct {
	Input  chan Record
	Output OutputData
	Config []byte `json:"config"`
}

type OutputInput struct {
	Plugin         Output
	Input          chan MetricData
	MetricDataDesc MetricMetadata
}
