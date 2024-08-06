package rparser

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestGetSqlType(t *testing.T) {
	p := &Parser{}
	t1 := p.GetSqlType("SELECT * FROM TABLE T")
	if t1 != SELECT {
		t.Fatal(t1)
	}
	t2 := p.GetSqlType("INSERT INTO t1 VALUES(1,2,3)")
	if t2 != INSERT {
		t.Fatal(t2)
	}
	t3 := p.GetSqlType("HHH")
	if t3 != UNKNOWN {
		t.Fatal(t3)
	}

	t4 := p.GetSqlType("SELECT * FROM foo Where id < 3")
	if t4 != SELECT {
		t.Fatal(t4)
	}

	t5 := p.GetSqlType("INSERT INTO foo VALUES (1,2,3)")
	if t5 != INSERT {
		t.Fatal(t5)
	}

	t6 := p.GetSqlType("UPSERT INTO foo VALUES (1,2,3)")
	if t6 != UNKNOWN {
		t.Fatal(t6)
	}

	fmt.Println(t1, t2, t3, t4, t5, t6)
}

func TestParserSelect(t *testing.T) {
	var p *Parser
	Convey("SELECT SQL", t, func() {
		p = &Parser{}
		ast, err := p.ParseSelect("SELECT ab,b, c FROM foo Where id < 3")
		So(err, ShouldBeNil)
		So(ast.ProjColumns, ShouldResemble, []string{"ab", "b", "c"}) // 投影列
		So(ast.Table, ShouldEqual, "foo")   // 表 foo

		p = &Parser{}
		ast, err = p.ParseSelect("SELECT ab,b, c FROM foo LIMIT 3")
		So(err, ShouldBeNil)
		So(ast.ProjColumns, ShouldResemble, []string{"ab", "b", "c"})
		So(ast.Table, ShouldEqual, "foo")
		So(ast.Limit, ShouldEqual, 3)

		p = &Parser{}
		ast, err = p.ParseSelect("SELECT ab,b,c FROM foo Where id < 3 AND ab > 10 LIMIT 11")
		So(err, ShouldBeNil)
		So(ast.ProjColumns, ShouldResemble, []string{"ab", "b", "c"})
		So(ast.Where, ShouldResemble, []string{"id", "<", "3", "AND", "ab", ">", "10"}) // 过滤
		So(ast.Table, ShouldEqual, "foo")
		So(ast.Limit, ShouldEqual, 11)

		p = &Parser{}
		ast, err = p.ParseSelect("SELECT 1")
		So(err, ShouldBeNil)
		So(ast.ProjColumns, ShouldResemble, []string{"1"})
		So(ast.Table, ShouldEqual, "")
	})
}

func TestParserInsert(t *testing.T) {
	var p *Parser
	Convey("INSERT SQL with Column names", t, func() {
		p = &Parser{}
		ast, err := p.ParseInsert("INSERT INTO table_name(column1, column2) VALUES (value1, value2)")
		So(err, ShouldBeNil)
		So(ast.Table, ShouldEqual, "table_name")
		So(ast.Columns, ShouldResemble, []string{"column1", "column2"})
		So(ast.Values, ShouldResemble, [][]string{{"value1", "value2"}})
	})

	Convey("column count miss match", t, func() {
		p = &Parser{}
		_, err := p.ParseInsert("INSERT INTO table_name(column1, column2, column3) VALUES (value1, value2)")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "INSERT INTO table_name(column1, column2, column3) VALUES (value1, value2) column and values not match") // values 数量不对
	})

	Convey("INSERT SQL", t, func() {
		p = &Parser{}
		ast, err := p.ParseInsert("INSERT INTO table_name VALUES (value1, value2)")
		So(err, ShouldBeNil)
		So(ast.Table, ShouldEqual, "table_name")
		So(ast.Columns, ShouldBeNil)
		So(ast.Values, ShouldResemble, [][]string{{"value1", "value2"}})
	})

	Convey("INSERT multiple rows", t, func() {
		p = &Parser{}
		ast, err := p.ParseInsert("INSERT INTO table_name VALUES (\"value1\", value2), (\"value3\", value4)")
		So(err, ShouldBeNil)
		So(ast.Table, ShouldEqual, "table_name")
		So(ast.Columns, ShouldBeNil)
		So(ast.Values, ShouldResemble, [][]string{{"\"value1\"", "value2"}, {"\"value3\"", "value4"}})
	})

	Convey("INSERT multiple rows 2", t, func() {
		p = &Parser{}
		ast, err := p.ParseInsert("INSERT INTO table (id, username, email) VALUES " +
			"(0, auxten, \"auxtenwpc@gmail.com\")," +
			"(1, hahaha, \"hahaha@gmail.com\")," +
			"(2, jijiji, \"jijiji@gmail.com\")")
		So(err, ShouldBeNil)
		So(ast.Table, ShouldEqual, "table")
		So(ast.Columns, ShouldResemble, []string{"id", "username", "email"})
		So(ast.Values, ShouldResemble, [][]string{{"0", "auxten", "\"auxtenwpc@gmail.com\""},
			{"1", "hahaha", "\"hahaha@gmail.com\""}, {"2", "jijiji", "\"jijiji@gmail.com\""}})
	})

	Convey("column count miss match 2", t, func() {
		p = &Parser{}
		_, err := p.ParseInsert("INSERT INTO table_name VALUES (value1, value2), (value3, value4, value5)")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "INSERT INTO table_name VALUES (value1, value2), (value3, value4, value5) column and values not match") // 两个values 不等
	})
}



