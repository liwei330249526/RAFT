package planner

import (
	"course/shardkv"
	"course/sql/parser"
	"fmt"
	"go/token"
	"go/types"
	"strconv"
	"strings"
)

func GetKey(tableName string, rowId string) string {
	key := "t_" + tableName+ "_r_" + rowId
	return key
}

func GetMaxRowIdKey(tableName string) string {
	key := "t_" + tableName+ "MaxRowId"
	return key
}

type Plan struct {
	insAst *parser.InsertAst
	selAst *parser.SelectAst
	client *shardkv.SqlDbClient
	rowDataOut chan *Row
	filteredDataOut chan *Row
	limitedDataOut chan *Row
}

func NewPlan() *Plan {
	return &Plan{
		client : shardkv.NewConfig(),
		rowDataOut: make(chan *Row),
		filteredDataOut: make(chan *Row),
		limitedDataOut: make(chan *Row),
	}
}

func (p *Plan)InitChan() {
	p.rowDataOut = make(chan *Row)
	p.filteredDataOut = make(chan *Row)
	p.limitedDataOut = make(chan *Row)
}

func (p *Plan)SetInsert(insAst *parser.InsertAst) {
	p.insAst = insAst
	return
}

func (p *Plan)SetSelect(selAst *parser.SelectAst) {
	p.selAst = selAst
	return

}

// 获取所有数据
func (p *Plan)getRowData(out chan *Row) {
	// 构造 key, 遍历key，直到没有数据
	maxRowId := p.getMaxRowId(p.selAst.Table)
	defer close(out)
	for rowId := 0; rowId <= maxRowId; rowId++{
		rowIdStr := strconv.Itoa(rowId)
		key := GetKey(p.selAst.Table, rowIdStr)
		val := p.client.Ck.Get(key)
		if val == "" {
			continue
		}
		valRow := &Row{}
		valRow.GetData(val)
		out <- valRow
	}
}

// 获取数据
func (p *Plan)filteredData(in chan *Row, out chan *Row) {
	// 遍历in chan， 获取所有数据， 做 where 过滤操作， 发给out chan
	defer close(out)
	for row := range in {
		if len(p.selAst.Where) == 0 {
			out <- row
			continue
		}

		filtered, err :=  p.isRowFiltered(row)
		if err != nil {
			// 错误输出
			return
		}

		if !filtered {
			out <- row
		}
	}
}

/*
	Id       string
	Sex      string
	Age      string
	Username string
	Email    string
	Phone    string
}
*/
func (p *Plan)isRowFiltered(row *Row) (filtered bool, err error) {
	where := p.selAst.Where
	whereB := make([]string, len(where))
	copy(whereB, where)
	for i := 0; i < len(whereB); i++ {
		tok := strings.ToUpper(whereB[i])
		switch tok {
		case "AND":
			whereB[i] = "&&"
		case "OR":
			whereB[i] = "||"
		case "ID":
			whereB[i] = row.Id
		case "SEX":
			whereB[i] = row.Sex
		case "AGE":
			whereB[i] = row.Age
		case "USERNAME":
			whereB[i] = row.Username
		case "EMAIL":
			whereB[i] = row.Email
		case "PHONE":
			whereB[i] = row.Phone
		}
	}
	exprs := strings.Join(whereB, " ")

	var tv   types.TypeAndValue
	fSet := token.NewFileSet()
	if tv, err = types.Eval(fSet, nil, token.NoPos, exprs); err != nil {
		return
	}
	if tv.Type == nil {
		err = fmt.Errorf("eval(%q) got nil type but no error", exprs)
		return
	}
	tvTypeStr := tv.Type.String()
	if !strings.Contains(tvTypeStr, "bool") {
		err = fmt.Errorf("eval(%q) got non bool type", exprs)
		return
	}

	if tv.Value.ExactString() == "true" {
		filtered = false
	} else {
		filtered = true
	}
	return
}

func (p *Plan)limitData(in chan *Row, out chan *Row, limit int) {
	count := 0
	defer close(out)
	for row := range in {

		if count > limit && limit > 0 {

			count++
		}
		out <- row
	}
}

// 获取该表的最大rowId， key: t_xxMaxRowId
func (p *Plan)getMaxRowId(tableName string) int {
	key := GetMaxRowIdKey(tableName)
	val := p.client.Ck.Get(key)
	if val == "" {
		return 0
	}
	valNum , err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return valNum
}

func (p *Plan) setMaxRowId(tableName string, count int) {
	countStr := strconv.Itoa(count)
	key := GetMaxRowIdKey(tableName)
	p.client.Ck.Put(key, countStr)
	return
}



type Row struct {
	Id       string
	Sex      string
	Age      string
	Username string
	Email    string
	Phone    string
}

func (r *Row)RowVal() string {
	res := ""
	if r.Id != "" {
		res += r.Id + "_"
	}
	if r.Sex != "" {
		res += r.Sex + "_"
	}
	if r.Age != "" {
		res += r.Age + "_"
	}
	if r.Username != "" {
		res += r.Username + "_"
	}
	if r.Email != "" {
		res += r.Email + "_"
	}
	if r.Phone != "" {
		res += r.Phone + "_"
	}
	if res == "" {
		return ""
	} else {
		return res[:len(res)-1]
	}
}

func (r *Row)GetData(str string) {
	datas := strings.Split(str, "_")
	for i := 0; i < len(datas); i++ {
		if i == 0 {
			r.Id = datas[i]
		} else if i == 1 {
			r.Sex = datas[i]
		} else if i == 2 {
			r.Age = datas[i]
		} else if i == 3 {
			r.Username = datas[i]
		} else if i == 4 {
			r.Email = datas[i]
		} else if i == 5 {
			r.Phone = datas[i]
		}
	}
	return
}

// Insert 将 ast 的数据插入表
func (p *Plan)Insert() (count int, err error) {
	// 可以分配一个table id 代替table name, rowid, 默认从 0 开始，
	// todo: 计算一个最新的rowid
	// todo: 将表结构元数据存储起来
	maxRowId := p.getMaxRowId(p.insAst.Table)

	if len(p.insAst.Columns) == 0 {
		p.insAst.Columns = []string{"id", "sex", "age", "username", "email", "phone"}
	}
	for i := 0; i < len(p.insAst.Values); i++ {
		row := Row{}
		for j := 0; j < len(p.insAst.Columns); j++ {
			colName := p.insAst.Columns[j]
			switch strings.ToUpper(colName) {
			case "ID":
				row.Id = p.insAst.Values[i][j]
			case "SEX":
				row.Sex = p.insAst.Values[i][j]
			case "AGE":
				row.Age = p.insAst.Values[i][j]
			case "USERNAME":
				row.Username = p.insAst.Values[i][j]
			case "EMAIL":
				row.Email = p.insAst.Values[i][j]
			case "PHONE":
				row.Phone = p.insAst.Values[i][j]
			}
		}

		key := GetKey(p.insAst.Table, row.Id)
		val := row.RowVal()
		p.client.Ck.Put(key, val)
		rowId, _ := strconv.Atoi(row.Id)
		maxRowId = myMax(maxRowId, rowId)
		count++
	}
	p.setMaxRowId(p.insAst.Table, maxRowId)
	maxRowIdt := p.getMaxRowId(p.insAst.Table)
	fmt.Println(maxRowIdt)
	return 
}


// Insert 将 ast 的数据插入表
func (p *Plan)InsertRows(tableName string, rows []*Row) (count int, err error) {
	// 可以分配一个table id 代替table name, rowid, 默认从 0 开始，
	// todo: 计算一个最新的rowid
	// todo: 将表结构元数据存储起来
	maxRowId := p.getMaxRowId(tableName)

	for i := 0; i < len(rows); i++ {
		row := rows[i]
		key := GetKey(tableName, row.Id)
		val := row.RowVal()
		p.client.Ck.Put(key, val)
		rowId, _ := strconv.Atoi(row.Id)
		maxRowId = myMax(maxRowId, rowId)
		count++
	}
	p.setMaxRowId(tableName, maxRowId)
	return
}

func (p *Plan)Select() (out chan *Row,  err error) {
	// 获取所有行数数据, getRowData chan 发给 filter
	go p.getRowData(p.rowDataOut)
	go p.filteredData(p.rowDataOut, p.filteredDataOut)

	limit := p.selAst.Limit
	go p.limitData(p.filteredDataOut, p.limitedDataOut, limit)


	return p.limitedDataOut, nil
}

func myMax(a, b int) int {
	if a < b {
		return b
	}
	return a
}
