package filters

import (
	"github.com/rentiansheng/incenses/src/define"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

type Creator func() define.Filter

var outputs = map[string]Creator{}

func Add(name string, creator Creator) {
	outputs[name] = creator
}

func Get(name string) define.Filter {
	c := outputs[name]
	if c == nil {
		return nil
	}
	return c()
}
