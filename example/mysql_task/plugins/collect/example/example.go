package example

import (
	"encoding/json"
	"errors"

	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/define"
	"github.com/rentiansheng/incenses/src/plugins/collects"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/10/5
    @desc:

***************************/

func init() {
	collects.Add(name, func() define.Collect {
		return &example{}
	})
}

const (
	name = "collect_example"
)

type example struct {
	cfg config
}

type config struct {
}

func (e *example) Name() string {
	return name
}

func (e *example) Keys(ctx context.Context) ([]string, error) {
	// 聚合的key，例子是user
	// aggregate key, example is user
	return []string{"user1", "user2", "user3"}, nil
}

func (e *example) Run(ctx context.Context, key string, start, end uint64, input chan define.Record) error {
	datas, err := e.fetchData(ctx, key, start, end)
	if err != nil {
		return err
	}
	for _, row := range datas {
		input <- define.NewRecord(row.ToInput())
	}
	return nil
}

func (e *example) fetchData(ctx context.Context, key string, start, end uint64) ([]dataDesc, error) {
	if start != 1664553600 || end != 1667231999 {
		return nil, errors.New("not data")
	}
	/**** start simulation db sql ****/
	allData := make([]dataDesc, 0)
	if err := json.Unmarshal([]byte(testData), &allData); err != nil {
		return nil, err
	}

	results := make([]dataDesc, 0, len(allData))
	for _, item := range allData {
		if item.User == key {
			results = append(results, item)
		}
	}
	/****  end simulation db sql  ****/

	return results, nil
}

func (e *example) SetConfig(ctx context.Context, config []byte) error {
	if len(config) != 0 {
		return json.Unmarshal(config, &e.cfg)
	}
	return nil
}

func (e *example) Description() string {
	return `功能描述： example collect
参数描述:
`
}

var (
	_ define.Collect = (*example)(nil)
)
