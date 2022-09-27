package two_sum_field_rate

import (
	"encoding/json"
	"math"

	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/context/log"
	"github.com/rentiansheng/incenses/src/define"
	"github.com/rentiansheng/incenses/src/libs/rules/rule"
	"github.com/rentiansheng/incenses/src/plugins/aggregators"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/27
    @desc:

***************************/

const (
	name = "two_field_sum_rate"
)

func init() {
	aggregators.Add(name, func() define.Aggregator {
		return &twoFieldSumRate{}
	})
}

type twoFieldSumRate struct {
	config           config
	totalMolecular   float64
	totalDenominator float64
	metricKey        string
	extraValueArr    []string
	metricMetadata   define.MetricMetadata
}

type extraRule struct {
	Field     string `json:"field"`
	OutputKey string `json:"output_key"`
}

type config struct {
	Field struct {
		// 分子字段
		Molecular string `json:"molecular"`
		// 分母字段
		Denominator string `json:"denominator"`
	} `json:"field"`
	Rules     rule.Rules `json:"rules"`
	OutputKey string     `json:"output_key"`
	Extra     *extraRule `json:"extra"`
	Power     int        `json:"power"`
}

func (t twoFieldSumRate) Name() string {
	return name
}

func (t *twoFieldSumRate) Run(ctx context.Context, key string, data define.Record) error {
	t.metricKey = key
	dataMap := data.Data()
	fieldMap := data.Field()

	canTotal, err := t.config.Rules.Compare(key, dataMap)
	if err != nil {
		ctx.Log().
			Fields(log.Field("data", dataMap), log.Field("field", fieldMap), log.Field("rules", t.config.Rules)).
			Errorf("compare rule error. key: %s, err: %s", key, err.Error())
		return err
	}
	if !canTotal {
		// 不满足条件，不参与统计
		return nil
	}

	if t.config.Extra != nil {
		val := dataMap[t.config.Extra.Field]
		if val != "" && val != "nil" && val != "null" {
			t.extraValueArr = append(t.extraValueArr, val)
		}
	}
	t.totalMolecular += fieldMap[t.config.Field.Molecular]
	t.totalDenominator += fieldMap[t.config.Field.Denominator]

	return nil
}

func (t *twoFieldSumRate) SetConfig(ctx context.Context, config []byte) error {
	return json.Unmarshal(config, &t.config)
}

func (t twoFieldSumRate) Description() string {
	return `功能描述： 用来计算两个字段数据求和后的比率
参数描述: {"field":{"molecular":"", "denominator":""}, "rules":[]{"field":"", "value":"", "operator":""}, "output_key":"", "extra_rule":*{"field":"","output_key"":""}}
	output_key: 当前统计保存统计使用的名字
    field: 参与计算字段名
	field.molecular: 计算比率用分子字段
	field.denominator: 计算比率用分母字段
	rule: 计算的规则, 多个规则需要同时满足
	rule[x].field: 筛选数据要用到的字段
	rule[x].value: 筛选数据需要比较值。
	rule[x].operator: 判断筛选是否满足条件的规则，可选值:[equal,equal_key], equal: 等于，equal_key:是否等于key
	extra_rule: 指针类型，不存在的时候，没有附加需要存储的数据
	extra_rule.field: 需要存储数据的字段，
	extra_rule.output_key: 当前统计保存统计使用的名字，为空使用output_key

`
}

func (t twoFieldSumRate) Metric(ctx context.Context) (string, float64) {
	if float64(t.totalDenominator) == 0 {
		t.totalDenominator = 1
	}
	molecular := t.totalMolecular
	molecular = molecular * (math.Pow10(t.config.Power))

	// 改到用task 中定义的
	val := molecular / t.totalDenominator
	ctx.Log().Field("meta", t.metricMetadata).Debugf("molecular: %sv, denominator: %sv, val: %v",
		molecular, t.totalDenominator, val)
	return t.config.OutputKey, val
}

// MetricExtra 记录额外的值，
func (t twoFieldSumRate) MetricExtra(ctx context.Context) (string, interface{}, bool) {
	if t.config.Extra == nil {
		return "", nil, false
	}

	return t.config.Extra.OutputKey, t.extraValueArr, true
}

func (t *twoFieldSumRate) SetMetricMetadata(ctx context.Context, data define.MetricMetadata) error {
	t.metricMetadata = data
	return nil
}

var _ define.Aggregator = (*twoFieldSumRate)(nil)
