package mysql

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/define"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

type mysql struct {
	tableName string
	db        *gorm.DB
}

type dbTask struct {
	define.MetricTask

	// 修改人
	Modifier string `json:"modifier" gorm:"column:modifier;type:varchar(64);not null"`
	// 创建人
	Creator string `json:"creator" gorm:"column:creator;type:varchar(64);not null"`
	Mtime   uint64 `json:"mtime" gorm:"column:mtime;type:int(10) unsigned;not null"`
	Ctime   uint64 `json:"ctime" gorm:"column:ctime;type:int(10) unsigned;not null"`
}

func New(db *gorm.DB, tableName string) *mysql {
	return &mysql{
		tableName: tableName,
		db:        db,
	}
}

func (m mysql) Add(ctx context.Context, info define.MetricTask, extra map[string]interface{}) error {
	kv := info.Map()
	for key, val := range kv {
		extra[key] = val
	}
	return m.db.Table(m.tableName).Create(extra).Error

}

func (m mysql) ModifyOutputIndexName(ctx context.Context, taskName, indexName string) error {
	return m.db.Table(m.tableName).Where("task_name = ?", taskName).
		Update("output_index_name", indexName).Error
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (t *dbTask) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal metric power value:", value))
	}

	err := json.Unmarshal(bytes, t)
	return err
}

// Value return json value, implement driver.Valuer interface
func (t dbTask) Value() (driver.Value, error) {
	return json.Marshal(t)
}

func (m mysql) Get(ctx context.Context) ([]define.MetricTask, error) {
	tasks := make([]dbTask, 0)
	if err := m.db.Where("task_status = 1").Table(m.tableName).Find(&tasks).Error; err != nil {
		return nil, err
	}
	results := make([]define.MetricTask, len(tasks))
	for idx, task := range tasks {
		results[idx] = task.MetricTask
	}

	return results, nil
}

func (m mysql) TaskDone(ctx context.Context, name string, nextCycleTime, lastFinishTime uint64) error {
	doc := map[string]interface{}{
		"task_start":       nextCycleTime,
		"last_finish_time": lastFinishTime,
	}
	if err := m.db.Where("task_name = ?", name).Table(m.tableName).Updates(doc).Error; err != nil {
		return err
	}

	return nil
}

func (m mysql) InitTable(ctx context.Context) error {
	return m.db.Exec(CreateTableSQL(m.tableName)).Error
}

var _ define.MetricTaskImpl = (*mysql)(nil)
