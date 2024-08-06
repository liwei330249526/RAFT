package rparser

import (
	"course/parser"
	"fmt"
	"testing"
)

func TestYacc(t *testing.T) {
	sql := "select * from table t1;"
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return
	}
	fmt.Println(stmtNodes)

}