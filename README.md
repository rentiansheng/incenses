# Incenses(period calculate metric)

[中文文档](/README-zh.md)


Incenses is a statistical metrics framework based on periodic data collection, processing, aggregation and storage.
Based on the plugin system, it is easy to add support for the collection and statistics of data indicators in the cycle. 

There are four different types of plugins:

- Data collection (collect) plugin: collect data according to time from third-party data storage systems and APIs, eg: mysql, es, http service
- Data filter plugin: used for data conversion, filtering indicators, eg: splitting and merging field
- Data aggregation (aggregator) plugin: statistics on data, eg: count, sum 
- Data storage (output) plugin: Persist calculation results, eg: mysql

## function

- Cycle management, responsible for maintaining the current statistical cycle management. eg: cycle conversion, cycle switching
- Task management, supports horizontal expansion of tasks, distributed execution of tasks, and ensures that the same task is mutually exclusive on multiple nodes.
- Tasks are executed in batches, long task slices, task execution exceeds a certain time, and the task is suspended
- Module design. For data, business. Process logic decoupling to reduce complexity
- Data acceleration, the execution of data collection (collect) is accelerated by concurrency at the framework layer. Reduce development difficulty
- Data is automatically deduplicated


design thinking：
![设计思想](http://www.ireage.com/img/metrics/metric_plugins_config.png)
![metric_framework_function](http://www.ireage.com/img/metrics/metric_framework_function.png)

execute process：
The task execution process in the development perspective.
The data is executed in the order of collect → filter → aggregate → filter → output in one direction, and multiple plugins of the same type are executed in order

![metric_calculate_arch](http://www.ireage.com/img/metrics/metric_arch.png)
Data acceleration
![metric_framework_workers](http://www.ireage.com/img/metrics/metric_framework_workers.png)

## use

The user mainly invokes the socket through configuration.
