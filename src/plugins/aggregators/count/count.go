package count

import (
	"encoding/json"
	"fmt"

	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/context/log"
	"github.com/rentiansheng/incenses/src/define"
	"github.com/rentiansheng/incenses/src/libs/rules/compare"
	"github.com/rentiansheng/incenses/src/plugins/aggregators"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/27
    @desc:

***************************/

const (
	name = "count"
)

func init() {
	aggregators.Add(name, func() define.Aggregator {
		return &Count{}
	})
}

type rule struct {
	Field    string `json:"field"`
	Value    string `json:"value"`
	Operator string `json:"operator"`
}

type extraRule struct {
	Field     string `json:"field"`
	OutputKey string `json:"output_key"`
}

type configOption struct {
	OutputKey string `json:"output_key"`

	Rules     []rule     `json:"rules"`
	ExtraRule *extraRule `json:"extra_rule"`
}

type Count struct {
	hasFilter      bool
	config         []byte
	configOpt      configOption
	metricMetadata define.MetricMetadata

	value     float64
	extraRows []interface{}
}

func (c *Count) SetConfig(ctx context.Context, config []byte) error {

	if err := json.Unmarshal(config, &c.configOpt); err != nil {
		ctx.Log().Errorf("unmarshal config error. config: %s, err: %s", string(config), err.Error())
		return err
	}
	return nil
}

func (c *Count) Run(ctx context.Context, key string, record define.Record) error {
	rowData := record.Data()

	for _, rule := range c.configOpt.Rules {
		fieldValue := rowData[rule.Field]
		cmp, ok := compare.Handle[rule.Operator]
		if !ok {
			ctx.Log().Errorf("not operator. op: %s, rule: %#v", rule.Operator, rule)
			return fmt.Errorf("%s operator. unimplement", rule.Operator)
		}
		equal, err := cmp.Compare(key, fieldValue, rule.Value)
		if err != nil {
			ctx.Log().Fields(log.Field("data", rowData), log.Field("rule", rule)).
				Errorf("cmp error. op: %s, value: %s, err: %s",
					rule.Operator, rule.Value, err.Error())
			return err
		}
		if !equal {
			// 不满足条件，不计数
			return nil
		}
	}

	if c.configOpt.ExtraRule != nil {
		c.extraRows = append(c.extraRows, rowData[c.configOpt.ExtraRule.Field])
	}
	c.value += 1

	return nil
}

func (c *Count) Metric(ctx context.Context) (string, float64) {
	return c.configOpt.OutputKey, c.value
}

func (c *Count) MetricExtra(ctx context.Context) (string, interface{}, bool) {
	outputKey := c.configOpt.OutputKey
	if c.configOpt.ExtraRule == nil {
		return outputKey, c.extraRows, false
	}
	if c.configOpt.ExtraRule.OutputKey != "" {
		outputKey = c.configOpt.ExtraRule.OutputKey
	}
	return outputKey, c.extraRows, true
}

func (c *Count) Name() string {
	return "count"
}

func (c *Count) SetMetricMetadata(ctx context.Context, data define.MetricMetadata) error {
	c.metricMetadata = data
	return nil
}

func (c *Count) Description() string {
	return `功能描述： 用来统计满足条件的数据
参数描述: {"output_key":"", "rules":[]{"field":"", "value":"", "operator":""}, "extra_rule":*{field}}
	output_key: 当前统计保存统计使用的名字， 默认名字
	rule: 计算的规则, 多个规则需要同时满足
    rule[x].field: 筛选数据要用到的字段
	rule[x].value: 筛选数据需要比较值。
	rule[x].operator: 判断筛选是否满足条件的规则，可选值:[equal,equal_key], equal: 等于，equal_key:是否等于key
	extra: 指针类型，不存在的时候，没有附加需要存储的数据
	extra_rule.field: 需要存储数据的字段，
	extra_rule.output_key: 当前统计保存统计使用的名字，为空使用output_key

`
}

var (
	_ define.Aggregator = (*Count)(nil)
)
