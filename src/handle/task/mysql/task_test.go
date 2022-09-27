package mysql

import (
	"fmt"
	"gorm.io/gorm"
	"testing"
	"time"

	"github.com/orlangure/gnomock"
	gormMysql "gorm.io/driver/mysql"
	//	"gorm.io/gorm"
	"github.com/stretchr/testify/require"

	mockMysql "github.com/orlangure/gnomock/preset/mysql"
	"github.com/rentiansheng/incenses/src/context"
	"github.com/rentiansheng/incenses/src/define"
)

/***************************
    @author: tiansheng.ren
    @date: 2022/9/26
    @desc:

***************************/

func initMysql(t *testing.T) (m *mysql, deferFn func(), err error) {
	deferFn = func() {}
	tb := "metric_task_tab"
	mysqlUser, mysqlPWD, dbName := "metric", "metric", "metric"
	strCreateTableSQL := CreateTableSQL(tb)
	p := mockMysql.Preset(
		mockMysql.WithUser(mysqlUser, mysqlPWD),
		mockMysql.WithDatabase(dbName),
		mockMysql.WithQueries(strCreateTableSQL),
	)

	container, err := gnomock.Start(p)
	if err != nil {
		return nil, deferFn, err
	}
	// 必须返回
	deferFn = func() { _ = gnomock.Stop(container) }
	defer func() {
		if err != nil {
			deferFn()
		}
	}()
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", mysqlUser, mysqlPWD, container.DefaultAddress(), dbName)

	db, err := gorm.Open(gormMysql.Open(dsn))
	if err != nil {
		return nil, deferFn, err
	}
	m = &mysql{
		tableName: tb,
		db:        db,
	}
	return m, deferFn, nil
}

func TestMysqlGet(t *testing.T) {
	m, deferFn, err := initMysql(t)
	require.NoError(t, err, "mock mysql error")
	defer deferFn()

	tests := []struct {
		status []uint8
		output int
	}{
		{
			status: []uint8{1, 1, 2, 3, 1, 3, 1, 2, 2, 2, 2, 2},
			output: 4,
		},
		{
			status: []uint8{2, 2, 2, 2, 2},
			output: 0,
		},
		{
			status: []uint8{2},
			output: 0,
		},
		{
			status: []uint8{1},
			output: 1,
		},
	}

	for idx, tt := range tests {
		err = m.db.Table(m.tableName).Where("id > 0").Delete(nil).Error
		require.NoError(t, err, "clean task table. index: %d", idx)
		for statusIdx, status := range tt.status {
			row := buildTaskInfo(statusIdx, status)
			err := m.db.Table(m.tableName).Create(row).Error
			require.NoError(t, err, "create task. index: %d", idx)
			findRow := dbTask{}
			err = m.db.Table(m.tableName).Find(&findRow, "task_name = ?", row.TaskName).Error
			require.NoError(t, err, "find table. index: %d", idx)
			require.Equal(t, row, findRow, "find task error. test index: %d, status index: %d", idx, statusIdx)
		}
		tasks, err := m.Get(context.Background())
		require.NoError(t, err, "test task mysql get error. index: %d", idx)
		require.Equal(t, tt.output, len(tasks), "test task mysql row count. index: %d", idx)

	}

}

func TestMysqlTaskDone(t *testing.T) {
	m, deferFn, err := initMysql(t)
	require.NoError(t, err, "mock mysql error")
	defer deferFn()

	type args struct {
		nextCycleTime  uint64
		lastFinishTime uint64
	}
	tests := struct {
		input []args
	}{

		input: []args{
			{
				1,
				1,
			},
			{
				2,
				2,
			},
			{
				3,
				3000,
			},
			{
				uint64(time.Now().Unix()),
				uint64(time.Now().Unix()),
			},
		},
	}

	row := buildTaskInfo(1, 1)
	err = m.db.Table(m.tableName).Create(row).Error
	require.NoError(t, err, "create task error")

	for idx, tt := range tests.input {

		err := m.TaskDone(context.Background(), row.TaskName, tt.nextCycleTime, tt.lastFinishTime)
		require.NoError(t, err, "TaskDone table. index: %d", idx)

		findRow := dbTask{}
		err = m.db.Table(m.tableName).Find(&findRow, "task_name = ?", row.TaskName).Error
		require.NoError(t, err, "find table. index: %d", idx)
		require.Equal(t, tt.nextCycleTime, findRow.TaskStart, "equal task start  error. test index: %d", idx)
		require.Equal(t, tt.lastFinishTime, findRow.LastFinishTime, "equal task last finish time  error. test index: %d", idx)

	}
}

func buildTaskInfo(idx int, status uint8) dbTask {
	ts := uint64(time.Now().Unix())
	name := fmt.Sprintf("name-%d", idx)
	return dbTask{
		MetricTask: define.MetricTask{
			TaskName:   name,
			TaskCycle:  1,
			CycleMode:  1,
			TaskStatus: define.StatusEnumType(status),

			CalculateCycle:  1,
			TaskStart:       ts,
			Collect:         define.MetricTaskPluginCollectConfig{"xx", []byte("{}")},
			Filters:         nil,
			Aggregators:     nil,
			Output:          define.MetricTaskPluginOutputConfig{},
			LastFinishTime:  0,
			OutputIndexName: name,
		},
		Modifier: "test",
		Creator:  "test",
		Mtime:    ts,
		Ctime:    ts,
	}
}
