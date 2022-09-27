package compare

/***************************
    @author: tiansheng.ren
    @date: 2022/9/27
    @desc:

***************************/

type Compare interface {
	Compare(key, fieldValue, value string) (bool, error)
	String() string
	Operator() string
}

// Handle  比较方法集合
var Handle = map[string]Compare{
	"equal":     equalStrCmp(""),
	"equal_key": equalKeyCmp(""),
}

type equalStrCmp string

func (equalStrCmp) Compare(key string, fieldVale, value string) (bool, error) {
	return fieldVale == value, nil
}

func (equalStrCmp) Operator() string {
	return "equal"
}

func (equalStrCmp) String() string {
	return "equal_key: compare field value equal value"
}

type equalKeyCmp string

func (equalKeyCmp) Compare(key, fieldValue, value string) (bool, error) {
	return key == fieldValue, nil
}

func (equalKeyCmp) Operator() string {
	return "equal_key"
}

func (equalKeyCmp) String() string {
	return "equal_key: compare field key equal key"
}
