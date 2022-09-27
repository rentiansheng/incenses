package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/orlangure/gnomock"
	mysqlMock "github.com/orlangure/gnomock/preset/mysql"
	redisMock "github.com/orlangure/gnomock/preset/redis"
	gormMysql "gorm.io/driver/mysql"
	"gorm.io/gorm"

	_ "github.com/rentiansheng/incenses/example/mysql_task/plugins/collect/example"
	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/core"
	"github.com/rentiansheng/incenses/src/define"
	"github.com/rentiansheng/incenses/src/handle/task/mysql"
	outputMysql "github.com/rentiansheng/incenses/src/plugins/outputs/mysql"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/28
    @desc:

***************************/

func main() {
	ctx := context.TODO()

	tableNam := "metric_task_tab"
	mysqlUser, mysqlPWD, dbName := "metric", "metric", "metric"
	mysqlMockInst := mysqlMock.Preset(
		mysqlMock.WithUser(mysqlUser, mysqlPWD),
		mysqlMock.WithDatabase(dbName),
	)

	mysqlContainer, err := gnomock.Start(mysqlMockInst)
	if err != nil {
		fmt.Println("error", err)
		return
	}
	// 必须返回
	defer func() { _ = gnomock.Stop(mysqlContainer) }()

	redisMockInst := redisMock.Preset()
	redisContainer, err := gnomock.Start(redisMockInst)
	if err != nil {
		fmt.Println("error", err)
		return
	}
	// 必须返回
	defer func() { _ = gnomock.Stop(redisContainer) }()

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", mysqlUser, mysqlPWD, mysqlContainer.DefaultAddress(), dbName)

	mysqlTaskHandle, err := initExampleTask(ctx, dsn, tableNam)
	if err != nil {
		fmt.Println("initExampleTask error", err)
		return
	}

	cache := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    redisContainer.DefaultAddress(),
		DB:      0,
	})

	core.SetClient(cache)
	event, err := core.New(mysqlTaskHandle)
	if err != nil {
		fmt.Println("event init error", err)
		return
	}
	go func() {
		for {

			task, err := mysqlTaskHandle.Get(ctx)
			if err != nil {
				fmt.Println(err)
			}
			byteTask, _ := json.Marshal(task)
			fmt.Println(string(byteTask))
			time.Sleep(time.Minute)
		}

	}()

	event.Run()

}

func initTask(ctx context.Context, taskHandle define.MetricTaskImpl) error {
	aggregators := []string{
		`{"output_key":"rule1",
		"rules":[{"field":"label1","value":"label1","operator":"equal"},{"field":"label2","value":"label2","operator":"equal"}],
		"extra_rule":{"field":"id","output_key":"rule1"}}`,
		`{"output_key":"rule2",
		"rules":[{"field":"label5","value":"label5","operator":"equal"},{"field":"label4","value":"label4","operator":"equal"}],
		"extra_rule":{"field":"id","output_key":"rule2"}}`}

	info := define.MetricTask{
		TaskName:       "test example",
		TaskCycle:      define.TaskCycleTypeMonthly,
		CycleMode:      define.CycleModeTypeInnerDay,
		CalculateCycle: 1,
		TaskStatus:     define.StatusEnumTypeNormal,
		TaskStart:      1667231000,
		Collect:        define.MetricTaskPluginCollectConfig{Name: "collect_example", Config: (define.RAWConfig)("{}")},
		Filters:        nil,
		Aggregators: define.MetricTaskPluginAggregatorConfigArr{
			define.MetricTaskPluginAggregatorConfig{
				Name:   "count",
				Config: (define.RAWConfig)(aggregators[0]),
			},
			define.MetricTaskPluginAggregatorConfig{
				Name:   "count",
				Config: (define.RAWConfig)(aggregators[1]),
			},
		},
		Output:          define.MetricTaskPluginOutputConfig{Name: "mysql"},
		LastFinishTime:  0,
		OutputIndexName: "",
	}
	return taskHandle.Add(ctx, info, map[string]interface{}{
		"modifier": "test",
		"creator":  "test",
		"mtime":    time.Now().Unix(),
		"ctime":    time.Now().Unix(),
	})
}

func initExampleTask(ctx context.Context, MysqlDSN, tableName string) (define.MetricTaskImpl, error) {
	db, err := gorm.Open(gormMysql.Open(MysqlDSN))
	if err != nil {
		return nil, fmt.Errorf("connect db error. %s", err)
	}

	mysqlTaskHandle := mysql.New(db, tableName)

	/***  optional: start add calculate metric task ***/
	if err := mysqlTaskHandle.InitTable(ctx); err != nil {
		return nil, fmt.Errorf("init task table error")
	}
	/*	end add calculate metric task  */

	outputMysql.SetDB(db)

	/***  optional: start add storage calculate metric value ***/
	sqls := outputMysql.InitSQL(ctx)
	for _, sql := range sqls {
		if err := db.Exec(sql).Error; err != nil {
			return nil, fmt.Errorf("execute init output tab error. %s", err.Error())
		}
	}
	/*	end add storage calculate metric value  */

	if err := initTask(ctx, mysqlTaskHandle); err != nil {
		return nil, fmt.Errorf("init task info error. %s", err.Error())
	}
	return mysqlTaskHandle, nil
}
