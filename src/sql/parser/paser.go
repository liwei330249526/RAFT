package parser

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
	WHERE = "Where"
	LIMIT = "LIMIT"
	INSERT = "INSERT"
	INTO = "INTO"
	VALUES = "VALUSE"
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

func NewSelectAst() *SelectAst {
	sel := &SelectAst{
		ProjColumns: make([]string, 0),
		Where:       make([]string, 0),
	}
	return sel
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
		err = fmt.Errorf("%s not have WHERE1", sel)
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