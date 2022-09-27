# Incenses(period calculate metric)

 
Incenses 是一个基于周期数据收集、处理、聚合和存储的统计指标框架。
基于插件系统，能够轻松添加对周期内数据指标收集及统计的支持。有四种不同类型的插件：

- 数据采集(collect)插件： 从第三方数据存储系统及API 中按照时间进行数据采集，比如： mysql，es，http service
- 数据过滤(filter)插件: 用来做数据转换，过滤指标，比如：对已有字段拆分，合并，去重等
- 数据聚合(aggregator)插件：对数据进行统计，比如：计数，求和比例
- 数据存储(output)插件: 将计算结果持久化, 比如: mysql

## 主要功能

- 周期管理，负责维护当前正在做统计周期管理。 eg: 周期转换，周期切换
- 任务管理，支持任务水平扩展，任务分布式执行，保证同一个任务在多个节点上互斥。
- 任务分批执行，长任务切片, 任务执行超过一定时间，将任务暂停
- 模块化设计。对数据，业务。流程逻辑解耦，降低复杂度
- 数据加速， 对数据采集(collect)执行在框架层通过并发加速。减少开发难度
- 数据自动去重


设计思想：
![设计思想](http://www.ireage.com/img/metrics/metric_plugins_config.png)
![metric_framework_function](http://www.ireage.com/img/metrics/metric_framework_function.png)

执行流程：
在开发角度任务执行过程。
数据按照collect → filter → aggregate → filter → output 单方向顺序执行 同类型的多个插件按照顺序执行


![metric_calculate_arch](http://www.ireage.com/img/metrics/metric_arch.png)
数据加速实现
![metric_framework_workers](http://www.ireage.com/img/metrics/metric_framework_workers.png)

## 使用
 
使用者主要通过配置来调用插架。 

