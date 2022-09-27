package mysql

import "fmt"

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

const sqlSchema = "CREATE TABLE if not exists `%s` (" +
	"`id` int(11) NOT NULL AUTO_INCREMENT," +
	"`task_name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '任务的名字，同时也是指标名字'," +
	"`task_cycle` tinyint(8) NOT NULL COMMENT '指标周期（1年，2季，3月，4周，5日，6时）'," +
	"`cycle_mode` tinyint(8) NOT NULL COMMENT '周期执行方式，1 周期结束后执行，2周期中每天计算一次'," +
	"`calculate_cycle` tinyint(8) NOT NULL COMMENT '需要从start计算多少周期'," +
	"`output_index_name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'task calculate result storage index name'," +
	"`task_start` int(11) NOT NULL COMMENT '开始处理任务的时间， 有start+cycle 可以选出结束时间'," +
	"`task_status` tinyint(8) NOT NULL COMMENT '任务状态， 1 正常，可以允许， 2. 暂停，不被执行 3. 待删除 100.local task正在本地开发调试的任务'," +
	"`collect` json NOT NULL COMMENT '{Name string, Config []byte}'," +
	"`filters` json NOT NULL COMMENT '[]{Name string, Config []byte}'," +
	"`aggregators` json NOT NULL COMMENT '[]{Name string,Config []byte}'," +
	"`output` json NOT NULL COMMENT 'type{ Name string}'," +
	"`last_finish_time` int(10) unsigned DEFAULT NULL COMMENT 'Last execute finish time. Validate task is already executed in day.'," +
	"`power` varchar(512) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '{}'," +
	"`modifier` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL," +
	"`creator` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL," +
	"`mtime` int(10) unsigned NOT NULL," +
	"`ctime` int(10) unsigned NOT NULL," +
	"PRIMARY KEY (`id`)," +
	"UNIQUE KEY `uniq_Name` (`task_name`)" +
	") ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"

func CreateTableSQL(tb string) string {
	return fmt.Sprintf(sqlSchema, tb)
}
