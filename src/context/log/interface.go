package log

/***************************
    @author: tiansheng.ren
    @date: 2022/10/5
    @desc:

***************************/

type FieldImpl interface {
	Field() (string, interface{})
}

type logField struct {
	key string
	val interface{}
}

func (lf logField) Field() (string, interface{}) {
	return lf.key, lf.val
}

func Field(key string, val interface{}) FieldImpl {
	return &logField{
		key: key,
		val: val,
	}
}

type LogImpl interface {
	Field(string, interface{}) LogImpl
	Fields(fields ...FieldImpl) LogImpl
	Sync()
	Errorf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	Error(string)
	Info(string)
	Debug(string)
	Panicf(string, ...interface{})
}
