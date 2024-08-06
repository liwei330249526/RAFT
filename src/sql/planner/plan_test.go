package planner

import (
	"course/sql/rparser"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"go/token"
	"go/types"
	. "go/types"
	"strconv"
	"strings"
	"testing"
)

func TestEvalArith(t *testing.T) {
	var tests = []string{
		`true`,
		`false == false`,
		`12345678 + 87654321 == 99999999`,
		`10 * 20 == 200`,
		`(1<<1000)*2 >> 100 == 2<<900`,
		`"foo" + "bar" == "foobar"`,
		`"abc" <= "bcd"`,
		`len([10]struct{}{}) == 2*5`,
	}g
	fset := token.NewFileSet()
	for _, test := range tests {
		testEval(t, fset, nil, token.NoPos, test, Typ[UntypedBool], "", "true")
	}
}


func testEval(t *testing.T, fset *token.FileSet, pkg *Package, pos token.Pos, expr string, typ Type, typStr, valStr string) {
	gotTv, err := Eval(fset, pkg, pos, expr)
	if err != nil {
		t.Errorf("Eval(%q) failed: %s", expr, err)
		return
	}
	if gotTv.Type == nil {
		t.Errorf("Eval(%q) got nil type but no error", expr)
		return
	}

	// compare types
	if typ != nil {
		// we have a type, check identity
		if !Identical(gotTv.Type, typ) {
			t.Errorf("Eval(%q) got type %s, want %s", expr, gotTv.Type, typ)
			return
		}
	} else {
		// we have a string, compare type string
		gotStr := gotTv.Type.String()
		if gotStr != typStr {
			t.Errorf("Eval(%q) got type %s, want %s", expr, gotStr, typStr)
			return
		}
	}

	// compare values
	gotStr := ""
	if gotTv.Value != nil {
		gotStr = gotTv.Value.ExactString()
	}
	if gotStr != valStr {
		t.Errorf("Eval(%q) got value %s, want %s", expr, gotStr, valStr)
	}
}

// 测试表达式评价
func TestFunc(t *testing.T) {
	var tv   types.TypeAndValue
	var err error
	filtered := false
	fSet := token.NewFileSet()
	// 表达式，应该是false
	expr := "2 < 1"
	if tv, err = types.Eval(fSet, nil, token.NoPos, expr); err != nil {
		return
	}
	if tv.Type == nil {
		err = fmt.Errorf("eval(%q) got nil type but no error", expr)
		return
	}
	// 应该包含bool字段
	tvTypeStr := tv.Type.String()
	if !strings.Contains(tvTypeStr, "bool") {
		err = fmt.Errorf("eval(%q) got non bool type", expr)
		return
	}
    // 表达式结果应该是 true
	if tv.Value.ExactString() == "true" {
		filtered = false
	} else {
		filtered = true
	}
	println(filtered)
}


func TestPlan_Insert(t *testing.T) {
	Convey("Plan Insert one value", t, func() {
		// 计划
		plan := NewPlan()

		p := &rparser.Parser{}
		ast, err := p.ParseInsert("INSERT INTO table VALUES " +
			"(10, f, 28, auxten, \"auxtenwpc@gmail.com\", 13812341234)",
		)
		So(err, ShouldBeNil)
		plan.SetInsert(ast)

		cnt, err := plan.Insert()
		So(err, ShouldBeNil)
		So(cnt, ShouldEqual, 1)

		p2 := &rparser.Parser{}
		ast2, err := p2.ParseSelect("SELECT id, username, email FROM table")
		So(err, ShouldBeNil)

		plan.SetSelect(ast2)
		resultPipe, err := plan.Select()
		So(err, ShouldBeNil)
		cnt = 0
		var row *Row
		for row = range resultPipe {
			cnt++
			//fmt.Println(row.Id, string(row.Username[:]), string(row.Email[:]))
		}
		So(cnt, ShouldEqual, 1)
		So(row, ShouldNotBeNil)
		So(row.Id, ShouldEqual, "10")
		So(string(row.Username[:]), ShouldStartWith, "auxten")
		So(string(row.Email[:]), ShouldStartWith, "\"auxtenwpc@gmail.com\"")
		So(string(row.Phone[:]), ShouldStartWith, "13812341234")
	})
}


func TestPlan_Insert_multiple(t *testing.T) {
	Convey("Plan Insert multiple values", t, func() {

		// 计划
		plan := NewPlan()

		p := &rparser.Parser{}
		ast, err := p.ParseInsert("INSERT INTO table (id, username, email) VALUES " +
			"(0, auxten, \"auxtenwpc@gmail.com\")," +
			"(1, hahaha, \"hahaha@gmail.com\")," +
			"(2, aaaa, \"aaaa@gmail.com\")," +
			"(3, jijiji, \"jijiji@gmail.com\")",
		)
		So(err, ShouldBeNil)
		plan.SetInsert(ast)

		cnt, err := plan.Insert()
		So(err, ShouldBeNil)
		So(cnt, ShouldEqual, 4)

		p2 := &rparser.Parser{}
		ast2, err := p2.ParseSelect("SELECT id, username, email FROM table LIMIT 10")
		So(err, ShouldBeNil)
		plan.SetSelect(ast2)

		resultPipe, err := plan.Select()
		So(err, ShouldBeNil)
		id := 0
		fmt.Printf("\n")
		for row := range resultPipe {
			fmt.Printf("%d---, %s\n", id, row.RowVal())
			id++
		}
	})
}



func TestPlannerSelect(t *testing.T) {
	Convey("Volcano model select implementation", t, func() {
		tableName := "testTable1"
		const InsertCnt = 512

		rows := make([]*Row, 0)
		for i := uint32(0); i < InsertCnt; i++ {
			row := &Row{
				Id: strconv.Itoa(int(i)),
				Sex: func(i int) string {
					if uint8(i%2) == 1 {
						return "m"
					} else {
						return "f"
					}
				}(int(i)),
				Age:      strconv.Itoa(int(i % 120)) ,
				Username: string([]byte{'a', 'u', 'x', 't', 'e', 'n', byte('a' + i)}),
				Email:    string([]byte{'a', 'u', 'x', 't', 'e', 'n', '@', byte('a' + i)}),
				Phone:    string([]byte{'1', '2', '3', '4', '5', '6', '0' + uint8((i/100)%10), '0' + uint8((i/10)%10), '0' + uint8(i%10)}),
			}
			rows = append(rows, row)
		}
		plan := NewPlan()
		plan.InsertRows(tableName, rows)


		p := &rparser.Parser{}

		sql := fmt.Sprintf("SELECT id, username, email FROM %s WHERE id > 5 AND id < 7 LIMIT 3", tableName)
		ast, err := p.ParseSelect(sql)
		So(err, ShouldBeNil)

		plan.SetSelect(ast)
		resultPipe, err := plan.Select()
		So(err, ShouldBeNil)
		var i int
		for row := range resultPipe {
			fmt.Printf("%d---, %s\n", i, row.RowVal())
			i++
		}
		So(i, ShouldEqual, 1)


		plan.InitChan()
		p = &rparser.Parser{}

		sql = fmt.Sprintf("SELECT id, username, email FROM %s", tableName)
		ast, err = p.ParseSelect(sql)
		So(err, ShouldBeNil)

		plan.SetSelect(ast)
		resultPipe, err = plan.Select()
		So(err, ShouldBeNil)
		i = 0
		for row := range resultPipe {
			i++
			fmt.Printf("%d---, %s\n", i, row.RowVal())
		}
		So(i, ShouldEqual, InsertCnt)
	})
}