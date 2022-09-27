package example

import "fmt"

/***************************
    @author: tiansheng.ren
    @date: 2022/10/5
    @desc:

***************************/

const (
	testData = `[
{"id":1,"user":"user1","label1":"label1","label2":"label2","label3":"label3","label4":"label4","label5":"label5"},
{"id":2,"user":"user2","label1":"label1","label2":"label2","label3":"label3","label4":"label4","label5":"label5"},
{"id":3,"user":"user2","label1":"label1","label2":"label2","label3":"label3","label4":"label4","label5":"label5"},
{"id":4,"user":"user3","label1":"label1","label2":"label2","label3":"label3","label4":"label4","label5":"label5"},
{"id":5,"user":"user3","label1":"label1","label2":"label2","label3":"label3","label4":"label4","label5":"label5"},
{"id":6,"user":"user3","label1":"label1","label2":"label2","label3":"label3","label4":"label4","label5":"label5"}]`
)

type dataDesc struct {
	ID     uint64 `json:"id"`
	User   string `json:"user"`
	Label1 string `json:"label1"`
	Label2 string `json:"label2"`
	Label3 string `json:"label3"`
	Label4 string `json:"label4"`
	Label5 string `json:"label5"`
	Label6 string `json:"label6"`
}

func (d *dataDesc) ToInput() (string, map[string]string, map[string]float64) {
	uuid := fmt.Sprintf("%v", d.ID)
	labels := map[string]string{
		"user":   d.User,
		"label1": d.Label1,
		"label2": d.Label2,
		"label3": d.Label3,
		"label4": d.Label4,
		"label5": d.Label5,
		"label6": d.Label6,
		"id":     uuid,
	}
	return uuid, labels, nil
}
