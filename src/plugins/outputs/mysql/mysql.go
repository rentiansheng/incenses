package mysql

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	"gorm.io/gorm"

	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/context/log"
	"github.com/rentiansheng/incenses/src/define"
	"github.com/rentiansheng/incenses/src/libs/times"
	"github.com/rentiansheng/incenses/src/plugins/outputs"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/30
    @desc:

***************************/

const (
	name = "mysql"
	// 这里不能改，修改回影响数据查询和写入
	tableNum = 12
	//tableNameFormat
	tableNameFormat = "metric_%s_tab"
)

var (
	db *gorm.DB
)

func init() {
	outputs.Add(name, func() define.Output {
		return &Mysql{}
	})
}

func SetDB(client *gorm.DB) {
	db = client
	return
}

func InitSQL(ctx context.Context) []string {
	sqls := make([]string, tableNum)
	sqlSchema := "CREATE TABLE if not exists `%s` (" +
		"`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT," +
		"`metric_name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL," +
		"`metric_key` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL," +
		"`metric_value` json NOT NULL COMMENT '{“key”: num}'," +
		"`start_time` int(10) unsigned NOT NULL," +
		"`end_time` int(10) unsigned DEFAULT NULL," +
		"`extra` json DEFAULT NULL COMMENT '{“key”: any}'," +
		"`mtime` int(10) unsigned NOT NULL," +
		"`ctime` int(10) unsigned DEFAULT NULL," +
		"PRIMARY KEY (`id`)," +
		"KEY `idx_Name_StartTime_EndTime` (`metric_name`,`start_time`,`end_time`)," +
		"KEY `idx_Name_MetricKey_StartTime_EndTime` (`metric_name`,`metric_key`,`start_time`,`end_time`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
	for i := 0; i < tableNum; i++ {
		sqls[i] = fmt.Sprintf(sqlSchema, tableNameByID(uint32(i)))
	}

	return sqls

}

type Mysql struct {
	metricMetadata define.MetricMetadata
}

func (m Mysql) Name() string {
	return name
}

func (m Mysql) Write(ctx context.Context, data define.OutputData) error {

	saveData, err := convertOutputData(data, m.metricMetadata)
	if err != nil {
		ctx.Log().Errorf("convert data to store struct error. data: %#v, err: %s", data, err)
		return fmt.Errorf("convert data to store struct error. err: %s", err)
	}

	dataQuery := func() *gorm.DB {
		return db.Table(saveData.TableName()).Where("metric_name=? and metric_key=? and start_time=?",
			m.metricMetadata.MetricName, data.MetricKey, m.metricMetadata.Start)
	}
	var cnt int64
	err = dataQuery().Count(&cnt).Error
	if err != nil {
		ctx.Log().Errorf("mysql count execute error. data: %#v, err: %s", data, err)
		return err
	}
	if cnt == 0 {
		saveData.Ctime = saveData.Mtime
		if err := db.Create(saveData).Error; err != nil {
			ctx.Log().Errorf("mysql create execute error. data: %#v, err: %s", data, err)
			return err
		}
	} else {
		if err := dataQuery().Updates(saveData).Error; err != nil {
			ctx.Log().Errorf("mysql update execute error. data: %#v, err: %s", data, err)
			return err
		}
	}
	return nil
}

func (m Mysql) Exists(ctx context.Context, key string) (bool, error) {
	// 统计任务 last_finish_time > metric key 统计数据mtime.  当前key 统计结果是在上个统计的结果
	metricMetadata := m.metricMetadata
	// metric key 统计数据mtime < 当天开始的时间，当前key 统计结果在今天还没有进行统计，
	if metricMetadata.LastFinishTime < uint64(times.CurDayStartTimeStamp()) {
		return false, nil
	}

	paramData, err := convertOutputData(define.OutputData{}, m.metricMetadata)

	if err != nil {
		ctx.Log().Errorf("convert data to store struct error. data: %#v, err: %s", metricMetadata, err)
		return false, fmt.Errorf("convert data to store struct error. err: %s", err)

	}
	condData := map[string]interface{}{
		"metric_name": paramData.MetricName,
		"metric_key":  key,
		"start_time":  paramData.Start,
	}
	countQueryEngine := db.Table(metricMetadata.MetricName).Where(condData)

	// 如果指标已经存在的数据，大于指标上次完成指标计算的时间， 则证明改key 已经计算过了， 可以跳过
	countQueryEngine = countQueryEngine.Where("mtime > ?", metricMetadata.LastFinishTime)
	var cnt int64
	err = countQueryEngine.Count(&cnt).Error
	if err != nil {
		ctx.Log().Fields(log.Field("data", paramData), log.Field("meta", m.metricMetadata)).
			Errorf("mysql count execute error. err: %s", err)
		return false, err
	}
	return cnt > 0, nil
}

func (m Mysql) Description() string {
	return `功能描述: 将结果存放到分表mysql 中
	其他：
		指标数据数据存放的表，在任务 writer 字段中
`
}

func (m Mysql) IndexName(ctx context.Context) (string, error) {
	paramData, err := convertOutputData(define.OutputData{}, m.metricMetadata)
	if err != nil {
		ctx.Log().Field("data", m.metricMetadata).Errorf("get index error. err: %s", err.Error())
		return "", err
	}
	return paramData.TableName(), nil
}

func (m *Mysql) SetMetricMetadata(ctx context.Context, data define.MetricMetadata) error {
	m.metricMetadata = data
	return nil
}

type outputData struct {
	// 指标名字
	MetricName string `gorm:"column:metric_name"`
	MetricKey  string `gorm:"column:metric_key"`
	Value      string `gorm:"column:metric_value"`
	Extra      string `gorm:"column:extra"`
	Start      uint64 `gorm:"column:start_time"`
	End        uint64 `gorm:"column:end_time"`
	Ctime      uint64 `gorm:"column:ctime"`
	Mtime      uint64 `gorm:"column:mtime"`
}

func convertOutputData(data define.OutputData, meta define.MetricMetadata) (outputData, error) {
	/*
		values := make(map[string]float64, len(data.Value))
			for key, val := range data.Value {
			tmpVal := float64(val)
			// 对指标处理保留小数放到数据库中，
			if power, ok := meta.Power[key]; ok {
				tmpVal = tmpVal / math.Pow10(power)
			}
			values[key] = tmpVal

		}
	*/
	bytesVal, err := json.Marshal(data.Value)
	if err != nil {
		return outputData{}, err
	}
	bytesExtra, err := json.Marshal(data.Extra)
	if err != nil {
		return outputData{}, err
	}
	return outputData{
		MetricName: meta.MetricName,
		MetricKey:  data.MetricKey,
		Value:      string(bytesVal),
		Extra:      string(bytesExtra),
		Start:      meta.Start,
		End:        meta.End,
		Mtime:      uint64(time.Now().Unix()),
	}, nil
}

func (o outputData) TableName() string {
	idx := hashCode(o.MetricName) % 12
	return tableNameByID(idx)
}

// hashCode hashes using fnv32a algorithm
func hashCode(text string) uint32 {
	algorithm := fnv.New32a()
	algorithm.Write([]byte(text))
	return algorithm.Sum32()
}

type tableSuffix uint32

func (t tableSuffix) String() string {
	if t < 10 {
		return fmt.Sprintf("0%d", t)
	}
	return fmt.Sprintf("%d", t)
}

func tableNameByID(idx uint32) string {
	return fmt.Sprintf(tableNameFormat, tableSuffix(idx).String())
}

var (
	_ define.Output = (*Mysql)(nil)
)
