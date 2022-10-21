package core

import (
	gContext "context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v9"

	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/define"
	"github.com/rentiansheng/incenses/src/libs/redislock"
	timeCycle "github.com/rentiansheng/incenses/src/libs/time_cycle"
	_ "github.com/rentiansheng/incenses/src/plugins"
	"github.com/rentiansheng/incenses/src/plugins/aggregators"
	"github.com/rentiansheng/incenses/src/plugins/collects"
	"github.com/rentiansheng/incenses/src/plugins/filters"
	"github.com/rentiansheng/incenses/src/plugins/outputs"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

var cache *redis.Client

type event struct {
	collectWorkerNum int
	collectRetryNum  int
	// intervalDelay    time.Duration

	taskHandle define.MetricTaskImpl
	//lock define.Lock
}

func defaultEvent() event {
	return event{
		collectWorkerNum: 10,
		collectRetryNum:  2,
		//intervalDelay:    ,
		taskHandle: nil,
	}
}

func SetClient(c *redis.Client) {
	cache = c
}

func New(taskHandle define.MetricTaskImpl) (*event, error) {
	e := defaultEvent()
	if taskHandle == nil {
		return nil, errors.New("metric task handle not implement")
	}
	e.taskHandle = taskHandle
	if cache == nil {
		return nil, errors.New("redis handle not init")
	}
	redislock.SetClient(cache)
	return &e, nil
}

func (e event) Run(gctx gContext.Context) {
	ctx := context.NewContexts(gctx)
	// 处理quit信号
	e.cancel(ctx)

	// 与e.run 行程任务按顺序循环执行
	for {
		st := time.Now()
		e.run(ctx)

		execTs := time.Now().Sub(st)
		// 小于30 秒，delay 1s
		if execTs.Seconds() < 30 {
			time.Sleep(time.Second * 1)
		}
	}
}

func (e event) cancel(ctx context.Context) {
	cancelFn := ctx.Cancel()

	go func() {
		quit := make(chan os.Signal, 1)

		signal.Notify(quit)
		//等待接收到退出信号：
		<-quit
		ctx.Log().Info("Server is shutting down...")
		cancelFn()

	}()
}

func (e event) run(ctx context.Context) {

	defer ctx.Log().Sync()

	ctx.Log().Infof("start event")
	defer ctx.Log().Infof("end event")

	tasks, err := e.taskHandle.Get(ctx)
	if err != nil {
		ctx.Log().Errorf("get all task all error. err: %s", err.Error())
		return
	}

	for _, task := range tasks {

		if err := e.runTask(ctx, task); err != nil {
			ctx.Log().Field("task", task).Errorf("task execute error. err: %s", err.Error())
			continue
		}

	}

}

func (e event) runTask(ctx context.Context, taskInfo define.MetricTask) error {
	name := taskInfo.TaskName
	ctx = ctx.SubCtx(name)
	ctx.Log().Infof("start %s task", name)
	defer ctx.Log().Infof("end %s task", name)

	task, err := e.taskParams(ctx, taskInfo)
	if err != nil {
		ctx.Log().Field("task", taskInfo).Errorf("taskParams execute error. err: %s", err.Error())
		return err
	}
	if err := task.Run(ctx); err != nil {
		ctx.Log().Field("task", taskInfo).Errorf("task execute error. err: %s", err.Error())
		return err
	}
	return nil
}

func (e event) taskParams(ctx context.Context, taskInfo define.MetricTask) (*task, error) {
	taskInstance, err := e.initTaskInstance(ctx, taskInfo)
	if err != nil {
		return nil, err
	}

	taskName := taskInfo.TaskName
	collectPluginInstance := collects.Get(taskInfo.Collect.Name)
	if collectPluginInstance == nil {
		err := fmt.Errorf("collect plugin not found. task: %s, plugin name: %s", taskName, taskInfo.Collect.Name)
		ctx.Log().Error(err.Error())
		return nil, err
	}
	collConfig := taskInfo.Collect.Config
	if err := collectPluginInstance.SetConfig(ctx, []byte(collConfig)); err != nil {
		ctx.Log().Errorf("collect plugin set config error. task: %s, plugin name: %s, config: %s, err: %s",
			taskName, collectPluginInstance.Name, collConfig, err.Error())
		return nil, err
	}

	outputPluginInstance := outputs.Get(taskInfo.Output.Name)
	if outputPluginInstance == nil {
		err := fmt.Errorf("output plugin not found. task: %s, plugin name: %s", taskName, taskInfo.Output.Name)
		ctx.Log().Error(err.Error())
		return nil, err
	}

	taskInstance.collectPlugin = collectPluginInstance
	taskInstance.outputPlugin = outputPluginInstance

	for _, plugin := range taskInfo.Filters {
		f := filters.Get(plugin.Name)
		if f == nil {
			err := fmt.Errorf("filters plugin not found. task: %s, plugin name: %s", taskName, plugin.Name)
			ctx.Log().Error(err.Error())
			return nil, err
		}
		if err := f.SetConfig(ctx, []byte(plugin.Config)); err != nil {
			ctx.Log().Errorf("filters plugin set config error. task: %s, plugin name: %s, config: %s, err: %s",
				taskName, plugin.Name, plugin.Config, err.Error())
			return nil, err
		}
		taskInstance.filterPlugin = append(taskInstance.filterPlugin, f)
	}

	aggs, err := e.initAggregatorPlugin(ctx, taskInfo)
	if err != nil {
		return nil, err
	}

	taskInstance.aggregatorPlugin = aggs

	return taskInstance, nil

}

func (e event) initTaskInstance(ctx context.Context, taskInfo define.MetricTask) (*task, error) {
	taskName := taskInfo.TaskName

	timeCycles, err := e.initTaskInstanceCycles(ctx, taskInfo)
	if err != nil {
		ctx.Log().Errorf("get task cycle range error. err: %s", err.Error())
		return nil, err
	}

	taskInstance := &task{
		event:              &e,
		taskLastFinishTime: int64(taskInfo.LastFinishTime),
		name:               taskName,
		filterPlugin:       nil,
		aggregatorPlugin:   nil,

		indexName: taskInfo.OutputIndexName,
	}
	for _, cycle := range timeCycles {
		if cycle.Begin > uint64(taskInfo.TaskStart) {
			// 设置未最大时间周期
			taskInfo.TaskStart = cycle.Begin
		}
		taskInstance.metricMetadataArr = append(taskInstance.metricMetadataArr, define.MetricMetadata{
			MetricName:     taskName,
			Start:          cycle.Begin,
			End:            cycle.End,
			Cycle:          taskInfo.TaskCycle,
			CycleMode:      taskInfo.CycleMode,
			LastFinishTime: taskInfo.LastFinishTime,
		})
	}

	return taskInstance, nil
}

func (e event) initAggregatorPlugin(ctx context.Context, taskInfo define.MetricTask) ([]AggregatorFn, error) {
	aggsPlugins := make([]AggregatorFn, 0, len(taskInfo.Aggregators))
	taskName := taskInfo.TaskName

	for _, plugin := range taskInfo.Aggregators {
		fnPlugin := plugin
		aggFn := func(fCtx context.Context) (define.Aggregator, error) {
			f := aggregators.Get(fnPlugin.Name)
			if f == nil {
				err := fmt.Errorf("aggregator plugin not found. task: %s, plugin name: %s", taskName, plugin.Name)
				ctx.Log().Error(err.Error())
				return nil, err
			}

			if err := f.SetConfig(ctx, []byte(fnPlugin.Config)); err != nil {
				ctx.Log().Errorf("aggregator plugin set config error. task: %s, plugin name: %s, config: %s, err: %s",
					taskName, fnPlugin.Name, string(fnPlugin.Config), err.Error())
				return nil, err
			}

			return f, nil
		}

		// 校验配置是否有问题
		if _, err := aggFn(ctx); err != nil {
			return nil, err
		}

		aggsPlugins = append(aggsPlugins, aggFn)
	}

	return aggsPlugins, nil
}

func (e event) initTaskInstanceCycles(ctx context.Context, taskInfo define.MetricTask) ([]timeCycle.TimeInterval, error) {

	timeRange := make([]timeCycle.TimeInterval, 0, taskInfo.CalculateCycle)
	// db 中的start 是最后一个周期
	start := taskInfo.TaskStart
	for idx := 0; idx < int(taskInfo.CalculateCycle); idx++ {
		cycleRange, err := timeCycle.GetTimeInterval(start, timeCycle.CycleType(taskInfo.TaskCycle))
		if err != nil {
			ctx.Log().Errorf("get task cycle range error. err: %s", err.Error())
			return nil, err
		}
		start = cycleRange.Begin - 1
		timeRange = append(timeRange, cycleRange)
	}

	return timeRange, nil
}
