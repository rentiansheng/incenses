package core

import (
	osContent "context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/context/log"
	"github.com/rentiansheng/incenses/src/define"
	"github.com/rentiansheng/incenses/src/libs/redislock"
	"github.com/rentiansheng/incenses/src/libs/retry"
	"github.com/rentiansheng/incenses/src/libs/time_cycle"
	"github.com/rentiansheng/incenses/src/libs/times"
	"github.com/rentiansheng/incenses/src/libs/worker"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

var (
	// TODO: 全局整体限流
	// runLimit
	expireTaskDuration     = time.Minute * 10
	expireTaskLockDuration = time.Minute * 12
)

type AggregatorFn func(fCtx context.Context) (define.Aggregator, error)

type task struct {
	event              *event
	taskLastFinishTime int64
	// 任务的名字
	name string
	// 需要使用到的字段
	collectFields []string
	// 需要统计的数据原来插件名字
	collectPlugin define.Collect
	// 保存数据需要使用到的插件
	outputPlugin define.Output
	// 对数据进行筛选需要使用到的插件
	filterPlugin []define.Filter
	// 数据统计使用的插件，一个插件产生一条数据
	// 聚合差价不能公用，每个metric_key 需要使用单个差价
	aggregatorPlugin []AggregatorFn
	// 描述指标信息, 需要保证第一个元素是metric_task_tab 表中start 时间所在的周期，后续判断周期内，是否需要切换到下一个周期，依赖这个元素
	metricMetadataArr []define.MetricMetadata

	// filterPluginConfig     []model.MetricTaskPluginConfig
	// aggregatorPluginConfig []model.MetricTaskPluginConfig
	// 计算结果的来源
	// outputDataChn chan define.MetricData

	// 取消任务，context, 出现错误时候，取消整个任务所有插件协程任务及collect插件中根据分组key 开启的协程
	ctxCancelFn osContent.CancelFunc
	// 任务是否执行成功
	taskSuccess bool
	// 保证taskSuccess 修改只有一次
	taskStatusFailureOnce sync.Once

	// taskDoneSignal 判断任务是否已经结束，因为一个task任务会有多个协程同时执行， 并且collect插件同时也会有多个分组任务key同时执行组成
	taskDoneSignal sync.WaitGroup

	// 执行成功是否可以转移到下一个周期
	canNextCycle bool

	indexName string

	keyCnt int
}

func (t *task) Run(ctx context.Context) (err error) {
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			err = fmt.Errorf("panic: %#v", panicErr)
			debug.PrintStack()
		}

	}()
	if t.event == nil {
		event := defaultEvent()
		t.event = &event
	}
	ctx.WithTimeout(expireTaskDuration)
	locked, err := redislock.Lock(ctx, t.redisLockKey(), expireTaskLockDuration)
	if err != nil {
		ctx.Log().Errorf("get task locked error. name: %s, err: %s", t.name, err.Error())
		return err
	}
	if !locked {
		ctx.Log().Debugf("skip, name: %s", t.name)
		return nil
	}
	defer func() {
		if err := redislock.Unlock(ctx, t.redisLockKey()); err != nil {
			ctx.Log().Errorf("release task locked error. name: %s, err: %s", t.name, err.Error())
		}
	}()

	// 判断统计周期，是否可以执行
	if !t.canExecCycle(ctx) {
		// 结束周期时间没有到。执行下一个指标
		return nil
	}

	if err := t.ModifyOutputIndexName(ctx); err != nil {
		return err
	}

	t.taskSuccess = true
	// 设置人去取消方法，取消的时候会将任务执行状态设置未false，不需要更新db中的数据
	t.ctxCancelFn = t.TaskStatusFailure(ctx.Cancel())

	if err := t.iterativeCycle(ctx); err != nil {
		return err
	}
	// 超时或者任务被取消了。不需要更新db周期数据
	if ctx.Err() != nil {
		ctx.Log().Errorf("task context error. err: %s", ctx.Err())
		return err
	}

	return t.taskDone(ctx)
}

func (t *task) ModifyOutputIndexName(ctx context.Context) error {
	if t.outputPlugin == nil {
		ctx.Log().Errorf("output plugin is nil. name: %s", t.name)
		return fmt.Errorf("output plugin is nil")
	}
	if len(t.metricMetadataArr) == 0 {
		ctx.Log().Errorf("metric metadata is nil. name: %s", t.name)
		return fmt.Errorf("metric metadata is nil")
	}
	outputIndexName, err := t.outputPlugin.IndexName(ctx)
	if err != nil {
		ctx.Log().Errorf("not found metric table name, execute continue. name: %s, err: %s", t.name, err.Error())
		return err
	}
	if outputIndexName != t.indexName {
		// 错误不影响最终结果，跳过
		if err := t.event.taskHandle.ModifyOutputIndexName(ctx, t.name, outputIndexName); err != nil {
			ctx.Log().Errorf("update metric table name, execute continue. name: %s, err: %s", t.name, err.Error())
			return nil
		}
	}

	return nil
}

func (t *task) iterativeCycle(ctx context.Context) error {
	for _, metricMetadata := range t.metricMetadataArr {
		OutputPluginChn := make(chan define.MetricData, 100)
		oi := define.OutputInput{
			Plugin:         t.outputPlugin,
			Input:          OutputPluginChn,
			MetricDataDesc: metricMetadata,
		}

		if err := t.outputPlugin.SetMetricMetadata(ctx, metricMetadata); err != nil {
			ctx.Log().Field("metric metadata", metricMetadata).Errorf("set output plugin metric metadata error. err: %s",
				err.Error())
			return err
		}

		ci := define.CollectInput{
			Plugin:          t.collectPlugin,
			Fields:          t.collectFields,
			OutputPluginChn: OutputPluginChn,
			MetricMetadata:  metricMetadata,
		}

		// 启动数据收集插件
		t.execCollect(ctx, ci)

		// 用来接受需要保存的数据
		// 处理需要保存的数据
		if err := t.execOutput(ctx, oi); err != nil {
			return err
		}

	}

	return nil

}

func (t *task) taskDone(ctx context.Context) error {
	if t.taskSuccess && !ctx.IsDone() {

		// 找到对应周期，这个周期是固定的第一个元素，不能修改。
		metricMetadata := t.metricMetadataArr[0]
		begin := metricMetadata.Start
		lastFinishTime := uint64(time.Now().Unix())
		if t.canNextCycle {
			newCycleTimeMiddle := metricMetadata.End + (metricMetadata.End-metricMetadata.Start)/2
			nextCycleRange, err := time_cycle.GetTimeInterval(newCycleTimeMiddle, time_cycle.CycleType(metricMetadata.Cycle))
			if err != nil {
				ctx.Log().Errorf("get task cycle range error. err: %s", err.Error())
				return err
			}
			begin = nextCycleRange.Begin
			// 周期切换需要计算最新周期的数据
			lastFinishTime = 0
		}

		// 周期切换需要计算最新周期的数据
		if err := t.event.taskHandle.TaskDone(ctx, t.name, begin, lastFinishTime); err != nil {
			ctx.Log().Errorf("update task cycle time range error.")
			return nil
		}
	}
	return nil
}

// 根据周期判断任务是否可以执行
func (t *task) canExecCycle(ctx context.Context) bool {
	metricMetadata := t.metricMetadataArr[0]
	if metricMetadata.End < uint64(time.Now().Unix()) {
		// 周期结束，可以转移到下一个周期
		t.canNextCycle = true
		// 判断统计周期，是否已经到。当前时间大于统计周期
		return true
	}
	// 每次都需要执行的任务
	if metricMetadata.CycleMode == define.CycleModeTypeAlways {
		return true
	}
	if metricMetadata.CycleMode == define.CycleModeTypeInnerDay {
		if times.CurDayStartTimeStamp() > t.taskLastFinishTime /*int64(t.dbTask.Mtime) */ {
			return true
		}
	}

	return false
}

// execCollect 执行数据收集插件
func (t *task) execCollect(ctx context.Context, input define.CollectInput) {
	t.taskDoneSignal.Add(1)
	// 将协程放到execCollect 是为了让控制信号逻辑在一起，避免后续维护和调试成本
	go func() {
		defer func() {
			// 正在执行任务，是在调用开始execCollect 任务前结束
			// 收集数据结束，正在执行的任务-1.
			t.taskDoneSignal.Done()
		}()

		defer func() {
			panicErr := recover()
			if panicErr != nil {
				ctx.Log().Errorf("collect plugin error. err: %#v", panicErr)
				t.TaskStatusFailure(t.ctxCancelFn)
			}
		}()
		workers := worker.NewWaitExecWorker(t.event.collectWorkerNum)
		t.execCollectDataList(ctx, input, workers)
		if err := workers.Wait(); err != nil {
			return
		}
	}()

	return
}

// execCollectDataList 执行获取需要计算的数据
func (t *task) execCollectDataList(ctx context.Context, input define.CollectInput, workers worker.Worker) {
	// 获取需要处理指标key，分组数据
	keys, err := input.Plugin.Keys(ctx)
	if err != nil {
		ctx.Log().Errorf("get input keys error. name: %s, err: %s", input.Plugin.Name(), err.Error())
		// 取消任务，执行，无法获取数据
		t.ctxCancelFn()
		return
	}

	// TODO: 记录执行开始
	for idx, key := range keys {
		t.keyCnt = idx
		exists, err := t.outputPlugin.Exists(ctx, key)
		if err != nil {
			// 错误可以被忽略，最多是重复执行一次计算。
		}
		if exists {
			ctx.Log().Debugf("skip key. reason: exists value. key: %s, metric metadata: %#v", key, input.MetricMetadata)
			continue
		}
		tmpKey := key
		tmpCtx := ctx.SubCtx(tmpKey)
		// 通过chan链接插件， chan 在不同的插件中做in或者out实现。
		// eg： collect plugin中out 是filter plugin的in
		//      filter plugin 的out  是aggregator plugin 的in
		//      aggregator plugin 的out 是 output plugin 的in
		// 由于output 是整个task 任务公用，所在任务初期生成，
		// collect, filter,aggregator 使用到in,out都是分组内部key 生成，一个task 执行过程中，需要初始化多个
		collectChn := make(chan define.Record, 100)
		filterChn := make(chan define.Record, 100)
		cancelKeyWorkerFn := t.TaskStatusFailure(tmpCtx.Cancel())

		aggregatorPlugin := make([]define.Aggregator, len(t.aggregatorPlugin))
		for idx, plugin := range t.aggregatorPlugin {
			pluginInstance, err := plugin(ctx)
			if err != nil {
				ctx.Log().Errorf("aggregator plugin init error. key: %s, err: %s", key, err.Error())
				cancelKeyWorkerFn()
				continue
			}
			if err := pluginInstance.SetMetricMetadata(ctx, input.MetricMetadata); err != nil {
				cancelKeyWorkerFn()
				continue
			}
			aggregatorPlugin[idx] = pluginInstance
		}
		// 每个统计key单独使用一组chan 来完成
		// 生成 filter, aggregator,output 方法

		// 新加一个正在执行的任务, 正在执行filter
		// 取消信号代码在协程中的defer
		t.taskDoneSignal.Add(1)
		go t.execFilters(tmpCtx, define.FilterInput{
			Key:               tmpKey,
			Input:             collectChn,
			Output:            filterChn,
			Plugins:           t.filterPlugin,
			CancelKeyWorkerFn: cancelKeyWorkerFn,
		})

		// 新加一个正在执行的任务, 正在执行aggregator
		// 取消信号代码在协程中的defer
		t.taskDoneSignal.Add(1)
		go t.execAggregators(tmpCtx, define.AggregatorInput{
			Key:               tmpKey,
			Input:             filterChn,
			Output:            input.OutputPluginChn,
			Plugins:           aggregatorPlugin,
			CancelKeyWorkerFn: cancelKeyWorkerFn,
		})
		workers.Run(tmpCtx, func(fCtx osContent.Context) (retErr error) {
			defer func() {
				panicErr := recover()
				if panicErr != nil {
					retErr = fmt.Errorf("panic: %#v", panicErr)
					cancelKeyWorkerFn()
				}
				close(collectChn)
			}()

			fTaskCtx := context.NewContexts(fCtx)
			retErr = retry.DefaultRetry(func(idx int) (next bool, err error) {
				if err := input.Plugin.Run(fTaskCtx, tmpKey, input.MetricMetadata.Start, input.MetricMetadata.End, collectChn); err != nil {
					return true, err
				}
				return false, nil
			})
			if retErr != nil {
				fTaskCtx.Log().
					Fields(log.Field("task name", t.name), log.Field("MetricMetadata", input.MetricMetadata)).
					Errorf("execute input plugin error. err: %s", err)
				// 拉数据收集数据，出现问题，取消tmpKey 统计任务
				cancelKeyWorkerFn()
				return retErr
			}
			return nil
		})
	}

}

func (t *task) execFilters(ctx context.Context, input define.FilterInput) {
	// 需要传递过来
	defer func() {
		t.taskDoneSignal.Done()
		// 告诉下一个阶段，数据发送结束
		panicErr := recover()
		if panicErr != nil {
			ctx.Log().Panicf("execute filter error. name: %s, err: %#v", panicErr)
			input.CancelKeyWorkerFn()
		}
		close(input.Output)
	}()
	existUUIDMap := make(map[string]struct{}, 0)
	defer func() {
		existUUIDMap = nil
	}()
	for {
		select {
		case <-ctx.Done():
			ctx.Log().Infof("cancel plugin filter. context done. err: %v", ctx.Err())
			return
		case record, chnIsClose := <-input.Input:
			// 已经关闭chn，数据读取完了
			if !chnIsClose {
				return
			}
			// 去重
			if _, ok := existUUIDMap[record.UUID()]; ok {
				ctx.Log().Infof("duplicate record. key: %s, data: %#v, uuid: %s", input.Key, record.Data(), record.UUID())
				continue
			}
			// 记录数据
			existUUIDMap[record.UUID()] = struct{}{}
			if len(input.Plugins) == 0 {
				input.Output <- record
			} else {
				for _, filter := range input.Plugins {
					if err := filter.Run(ctx, input.Key, record); err != nil {
						// 出现错误，关闭任务
						input.CancelKeyWorkerFn()
						return
					}
				}
			}
		}
	}

}

func (t *task) execAggregators(ctx context.Context, input define.AggregatorInput) {
	ctx.Log().Infof("start aggregator key: %s", input.Key)
	defer ctx.Log().Infof("end aggregator key: %s", input.Key)

	defer func() {
		panicErr := recover()
		if panicErr != nil {
			ctx.Log().Panicf("panic: %#v", panicErr)
			input.CancelKeyWorkerFn()
		}
		t.taskDoneSignal.Done()
	}()
	for {

		select {
		case <-ctx.Done():
			ctx.Log().Infof("cancel plugin aggregators. context done. err: %v", ctx.Err())
			return
		case record, chnIsClose := <-input.Input:
			canExit, err := t.execAggregatorRow(ctx, record, chnIsClose, input)
			if err != nil {
				ctx.Log().Errorf("execute aggregator error. task name: %s, data: %#v, err: %s", t.name, record.Data(), err.Error())
				input.CancelKeyWorkerFn()
				return
			}
			if canExit {
				return
			}
		}
	}
}

func (t *task) execAggregatorRow(ctx context.Context, record define.Record, chnIsClose bool, input define.AggregatorInput) (bool, error) {
	// 已经关闭chan，数据读取完了
	if !chnIsClose {
		// 任务被取消，不需要保存数据
		if ctx.IsDone() {
			return true, ctx.Err()
		}
		outputData := define.MetricData{
			MetricKey: input.Key,
			Value:     make(map[string]float64, 0),
			Extra:     make(map[string]interface{}, 0),
		}

		for _, aggregator := range input.Plugins {

			metricItemName, metricValue := aggregator.Metric(ctx)
			ctx.Log().Debugf("aggregator single result. key: %s, plugin name: %s, field: %s, value: %s",
				input.Key, aggregator.Name(), metricItemName, metricValue)

			outputData.Value[metricItemName] = metricValue
			extraName, extraValue, exists := aggregator.MetricExtra(ctx)
			if exists {
				outputData.Extra[extraName] = extraValue
			}

		}
		ctx.Log().Field("output", outputData).Debugf("aggregator result")
		select {
		case <-ctx.Done():
		case input.Output <- outputData:

		}
		return true, nil

	}

	for _, aggregator := range input.Plugins {
		if err := aggregator.Run(ctx, input.Key, record); err != nil {
			// 出现错误，关闭任务
			input.CancelKeyWorkerFn()
			return false, err
		}
		// TODO: next 未实现
	}

	return false, nil
}

func (t *task) execOutput(ctx context.Context, input define.OutputInput) (err error) {
	go func() {
		// 任务处理完成
		t.taskDoneSignal.Wait()
		close(input.Input)
	}()

	defer func() {
		panicErr := recover()
		if panicErr != nil {
			err = fmt.Errorf("panic: %#v", panicErr)
			ctx.Log().Panicf("panic: %s", err.Error())

		}

	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case metricData, ok := <-input.Input:
			if !ok {
				// 数据处理完成
				return nil
			}
			if err := t.execOutputWrite(ctx, input, metricData); err != nil {
				return nil
			}
		}

	}
}

func (t *task) execOutputWrite(ctx context.Context, input define.OutputInput, metricData define.MetricData) (err error) {
	defer func() {
		if err != nil {
			// 执行出错，取消任务
			t.ctxCancelFn()
		}
	}()

	if err := input.Plugin.Write(ctx, define.OutputData{
		MetricData: metricData,
	}); err != nil {
		ctx.Log().Fields(log.Field("data", metricData), log.Field("metric data desc", input.MetricDataDesc)).
			Errorf("write metric error. name: %s, err: %s", input.Plugin.Name(), err.Error())
		return err
	}

	return nil

}

// TaskStatusFailure 取消任务的时候，表示任务中有统计失败，需要重试
func (t *task) TaskStatusFailure(cancelFunc osContent.CancelFunc) osContent.CancelFunc {
	return func() {
		t.taskStatusFailureOnce.Do(func() {
			t.taskSuccess = false
		})
		cancelFunc()
	}

}

func (t *task) redisLockKey() string {
	return define.LockKeyPrefix + t.name
}
