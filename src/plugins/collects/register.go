package collects

import (
	"github.com/rentiansheng/incenses/src/define"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

type Creator func() define.Collect

var inputs = map[string]Creator{}

func Add(name string, creator Creator) {
	inputs[name] = creator
}

func Get(name string) define.Collect {
	c := inputs[name]
	if c == nil {
		return nil
	}
	return c()
}
