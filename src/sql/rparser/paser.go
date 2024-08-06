package rparser

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
)

type SQLType string // 也可以用枚举
const (
	UNKNOWN = "UNKNOWN"
	SELECT = "SELECT"
	FROM = "FROM"
	WHERE = "WHERE"
	LIMIT = "LIMIT"
	INSERT = "INSERT"
	INTO = "INTO"
	VALUES = "VALUES"
	ALLX = "*"
)

// Parser 解析器
type Parser struct {
	s scanner.Scanner
}

// SelectAst select 抽象语法树
type SelectAst struct {
	// 投影
	ProjColumns []string
	// 表
	Table string
	// Where
	Where []string
	// 限制
	Limit int
}

type InsertAst struct {
	// 表
	Table string
	// 列
	Columns []string
	// 值, 可以是多行数据
	Values [][]string
}

func NewSelectAst() *SelectAst {
	sel := &SelectAst{
	}
	return sel
}

func NewInsertAst() *InsertAst {
	ins := &InsertAst{
	}
	return ins
}

// GetSqlType 获取 sql 语句类型
func (p *Parser)GetSqlType(s string) SQLType {
	r := strings.NewReader(s)
	p.s.Init(r)
	if ret := p.s.Scan(); ret == scanner.EOF {
		return UNKNOWN
	}
	if p.s.TokenText() == "SELECT" {
		return SELECT
	} else if p.s.TokenText() == "INSERT" {
		return INSERT
	}
	return UNKNOWN
}

// ParseSelect SELECT * FROM foo WHERE id < 3 LIMIT 1;
// SELECT ab,b, c FROM foo WHERE id < 3 LIMIT 1"
func (p *Parser) ParseSelect(sel string) (ast *SelectAst,err error) {
	ast = NewSelectAst()
	r := strings.NewReader(sel)
	p.s.Init(r)

	// 校验SELECT
	if ret := p.s.Scan(); ret == scanner.EOF {
		err = fmt.Errorf("%s not SELECT1", sel)
		return
	}

	if p.s.TokenText() != SELECT {
		err = fmt.Errorf("%s not SELECT2", sel)
		return
	}

	// 解析 *  或 ab, b, c ， 知道 FROM
	for {
		if ret := p.s.Scan(); ret == scanner.EOF {
			if len(ast.ProjColumns) == 0 {
				err = fmt.Errorf("%s not have project column", sel)  // 如果要结束了，还没有投影列，则报错
			}
			return
		}

		tok := p.s.TokenText()
		if tok == "," {
			continue
		} else if tok == ALLX {
			ast.ProjColumns = append(ast.ProjColumns, "*")
		} else if tok == FROM {
			break
		} else {
			ast.ProjColumns = append(ast.ProjColumns, tok)
		}
	}

	// 解析表名 foo
	if ret := p.s.Scan(); ret == scanner.EOF {
		err = fmt.Errorf("%s not have Table name", sel)
		return
	}
	ast.Table = p.s.TokenText()

	// 校验 Where
	if ret := p.s.Scan(); ret == scanner.EOF {
		//err = fmt.Errorf("%s not have WHERE1", sel)  // WHERE is not necessary
		return
	}

	if p.s.TokenText() == WHERE {  // 可以有 Where, 也可以没有WHERE
		for {
			if ret := p.s.Scan(); ret == scanner.EOF {
				if len(ast.Where) == 0 {
					err = fmt.Errorf("%s not have Where val", sel)
				}
				return
			}

			tok := p.s.TokenText()
			if tok == LIMIT {
				break
			} else {
				ast.Where = append(ast.Where, tok)
			}
		}
	} else if p.s.TokenText() != LIMIT {
		err = fmt.Errorf("%s not a normal sql ", sel)
		return
	}

	// 解析 Limit, 已经有limit了， 但没有 limit值， 错误
	if ret := p.s.Scan(); ret == scanner.EOF {
		err = fmt.Errorf("%s not have Limit val", sel)
		return
	}
	 limitVal, err := strconv.Atoi(p.s.TokenText())
	 if err != nil {
		 err = fmt.Errorf("%s Limit val err %s", sel, err)
		 return
	 }
	 ast.Limit = limitVal
	return
}

/*
ParseInsert
 	INSERT INTO table_name VALUES (value1, value2, …)
	or
	INSERT INTO table_name(column1, column2, …) VALUES (value1, value2, …)
*/
func (p *Parser) ParseInsert(strInsert string) (ist *InsertAst,err error) {
	ist = NewInsertAst()
	r := strings.NewReader(strInsert)
	p.s.Init(r)
	// 校验 INSERT
	if ret := p.s.Scan(); ret == scanner.EOF {
		err = fmt.Errorf("%s not INSERT1", strInsert)
		return
	}

	if p.s.TokenText() != INSERT {
		err = fmt.Errorf("%s not INSERT2", strInsert)
		return
	}

	// 校验 INTO
	if ret := p.s.Scan(); ret == scanner.EOF {
		err = fmt.Errorf("%s not have INTO", strInsert)
		return
	}

	if p.s.TokenText() != INTO {
		err = fmt.Errorf("%s not INTO2", strInsert)
		return
	}
	// Table name 表名
	if ret := p.s.Scan(); ret == scanner.EOF {
		err = fmt.Errorf("%s not have table name", strInsert)
		return
	}
	ist.Table = p.s.TokenText()

	// 如果是 (  获取素有 colmus ， 直到 VALUES
	if ret := p.s.Scan(); ret == scanner.EOF {
		err = fmt.Errorf("%s not have VALUES1", strInsert)
		return
	}
	if p.s.TokenText() == "(" {
		for {
			if ret := p.s.Scan(); ret == scanner.EOF {
				if len(ist.Columns) == 0 {
					err = fmt.Errorf("%s not have columns", strInsert)
				}
				return
			}

			tok := p.s.TokenText()
			if tok == VALUES {
				break
			} else if tok == ")" || tok == "," || tok == ""{
				continue
			} else {
				ist.Columns = append(ist.Columns, tok)
			}
		}

	} else if p.s.TokenText() != "VALUES" {
		// 不是 (, 一定是 VALUES
		err = fmt.Errorf("%s not VALUES2", strInsert)
		return
	}

	if ret := p.s.Scan(); ret == scanner.EOF {
		err = fmt.Errorf("%s not have VALUES1", strInsert)
		return
	}
	// 获取所有 values 值， 每行以 ( 开始， 以 ） 结束
	if p.s.TokenText() == "(" {
		for  {
			values := make([]string, 0)
			hasEnd := false
			for  {
				if ret := p.s.Scan(); ret == scanner.EOF {
					if len(ist.Values) == 0 {
						err = fmt.Errorf("%s not have values", strInsert)
					}
					hasEnd = true
					break
				}

				tok := p.s.TokenText()
				if tok == ")" {
					break
				} else if tok == "(" || tok == ")" || tok == "," || tok == "" {
					continue
				} else {
					values = append(values, tok)
				}
			}
			if len(values) != 0 {
				ist.Values = append(ist.Values, values)
			}
			if hasEnd {
				break
			}
		}
	}

	// 校验 columns 和 values 的长度
	colLen := len(ist.Columns)
	if colLen == 0 {
		colLen = len(ist.Values[0])
	}
	for i := 0; i < len(ist.Values); i++ {
		if len(ist.Values[i]) != colLen {
			err = fmt.Errorf("%s column and values not match", strInsert)
			return
		}
	}
	return
}