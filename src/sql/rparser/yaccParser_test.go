package rparser

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/parser/test_driver"
	"testing"
)

func TestYacc(t *testing.T) {
	sql := "select a,b from t1;"
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return
	}
	fmt.Println(stmtNodes)
}

type colX struct{
	colNames []string
}

func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
	fmt.Printf("%T\n", in)
	if name, ok := in.(*ast.ColumnName); ok {
		v.colNames = append(v.colNames, name.Name.O)
	}
	return in, false
}

func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func extract(rootNode *ast.StmtNode) []string {
	v := &colX{}
	(*rootNode).Accept(v)
	return v.colNames
}
func TestYacc2(t *testing.T) {
	sql := "select a,b from t1;"
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	fmt.Printf("%v\n", extract(&stmtNodes[0]))

	fmt.Println("end")
}