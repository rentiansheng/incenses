package rule

import (
	"fmt"

	"github.com/rentiansheng/incenses/src/libs/rules/compare"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/27
    @desc:

***************************/

type Rules []Rule

type Rule struct {
	Field    string `json:"field"`
	Value    string `json:"value"`
	Operator string `json:"operator"`
}

func (r Rules) Compare(metricKey string, dataMap map[string]string) (bool, error) {
	for _, rule := range r {
		fieldValue := dataMap[rule.Field]
		cmp, ok := compare.Handle[rule.Operator]
		if !ok {
			return false, fmt.Errorf("%s operator. unimplement", rule.Operator)
		}
		equal, err := cmp.Compare(metricKey, fieldValue, rule.Value)
		if err != nil {
			err = fmt.Errorf("cmp error. op: %s, rule: %#v, err: %s", rule.Operator, rule.Value, err.Error())
			return false, err
		}
		if !equal {
			// 不满足条件，
			return false, nil
		}
	}

	return true, nil
}
