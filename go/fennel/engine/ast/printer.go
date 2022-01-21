package ast

import (
	"fmt"
	"strings"
)

type Printer struct{}

var _ VisitorString = Printer{}

func (p Printer) VisitBinary(left Ast, op string, right Ast) string {
	return fmt.Sprintf("%s %s %s", left.AcceptString(p), op, right.AcceptString(p))
}

func (p Printer) VisitList(values []Ast) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for _, v := range values {
		sb.WriteString(fmt.Sprintf("%s, ", v.AcceptString(p)))
	}
	sb.WriteByte(']')
	return sb.String()
}

func (p Printer) VisitDict(values map[string]Ast) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for k, v := range values {
		sb.WriteString(fmt.Sprintf("%s=%s, ", k, v.AcceptString(p)))

	}
	sb.WriteByte('}')
	return sb.String()
}

func (p Printer) VisitTable(inner Ast) string {
	return fmt.Sprintf("table(%s)", inner.AcceptString(p))
}

func (p Printer) VisitOpcall(operand Ast, namespace, name string, kwargs Dict) string {
	return fmt.Sprintf("%s | %s.%s(%s)", operand.AcceptString(p), namespace, name, kwargs.AcceptString(p))
}

func (p Printer) VisitVar(name string) string {
	return fmt.Sprintf("$%s", name)
}

func (p Printer) VisitAtom(at AtomType, lexeme string) string {
	return lexeme
}

func (p Printer) VisitStatement(name string, body Ast) string {
	bodystr := body.AcceptString(p)
	if name != "" {
		return fmt.Sprintf("%s = %s;", name, bodystr)
	} else {
		return fmt.Sprintf("%s;", bodystr)
	}
}

func (p Printer) VisitQuery(statements []Statement) string {
	var sb strings.Builder
	for _, s := range statements {
		sb.WriteString(s.AcceptString(p))
		sb.WriteString("\n")
	}
	return sb.String()
}
