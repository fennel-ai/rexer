package ast

import (
	"fmt"
	"strings"
)

type Printer struct{}

func (p Printer) VisitHighFnCall(Type HighFnType, varname string, lambda Ast, iter Ast) string {
	return fmt.Sprintf("%s(lambda %s: %s, %s)", Type, varname, lambda.AcceptString(p), iter.AcceptString(p))
}

func (p Printer) VisitFnCall(module, name string, kwargs map[string]Ast) string {
	sb := strings.Builder{}
	for k, v := range kwargs {
		sb.WriteString(fmt.Sprintf("%s=%s, ", k, v.AcceptString(p)))
	}
	kstr := strings.TrimSuffix(sb.String(), ", ")
	return fmt.Sprintf("%s.%s(%s)", module, name, kstr)
}

func (p Printer) VisitLookup(on Ast, property string) string {
	return fmt.Sprintf("%s.%s", on.AcceptString(p), property)
}

func (p Printer) VisitAt() string {
	return "@"
}

var _ VisitorString = Printer{}

func (p Printer) VisitBinary(left Ast, op string, right Ast) string {
	if op == "[]" {
		return fmt.Sprintf("%s[%s]", left.AcceptString(p), right.AcceptString(p))
	} else {
		return fmt.Sprintf("%s %s %s", left.AcceptString(p), op, right.AcceptString(p))
	}
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

func (p Printer) VisitOpcall(operands []Ast, vars []string, namespace, name string, kwargs Dict) string {
	inputs := make([]string, len(operands))
	for i := range operands {
		inputs[i] = operands[i].AcceptString(p)
	}
	return fmt.Sprintf(
		"%s.%s(%s, vars=%s, kwargs=%s)",
		namespace,
		name,
		strings.Join(inputs, ","),
		strings.Join(vars, ","),
		kwargs.AcceptString(p),
	)
}

func (p Printer) VisitVar(name string) string {
	return fmt.Sprintf("$%s", name)
}

func (p Printer) VisitAtom(_ AtomType, lexeme string) string {
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

func (p Printer) VisitIfelse(condition Ast, thenDo Ast, elseDo Ast) string {
	return fmt.Sprintf("if %s then %s else %s",
		condition.AcceptString(p),
		thenDo.AcceptString(p),
		elseDo.AcceptString(p),
	)
}

func (t HighFnType) String() string {
	switch t {
	case Map:
		return "map"
	case Filter:
		return "filter"
	case GroupBy:
		return "groupby"
	case OrderBy:
		return "orderby"
	case Reduce:
		return "reduce"
	default:
		return fmt.Sprintf("unknown:%d", t)
	}
}
