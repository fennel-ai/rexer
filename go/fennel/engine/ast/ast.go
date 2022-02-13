package ast

import (
	"fennel/engine/ast/proto"
	"fennel/lib/value"
)

// TODO: make this generic instead of string
type VisitorString interface {
	VisitAtom(at AtomType, lexeme string) string
	VisitBinary(left Ast, op string, right Ast) string
	VisitList(values []Ast) string
	VisitDict(values map[string]Ast) string
	VisitTable(inner Ast) string
	VisitOpcall(operand Ast, namespace, name string, kwargs Dict) string
	VisitVar(name string) string
	VisitStatement(name string, body Ast) string
	VisitQuery(statements []Statement) string
	VisitAt() string
	VisitLookup(on Ast, property string) string
	VisitIfelse(condition Ast, thenDo Ast, elseDo Ast) string
}

type VisitorValue interface {
	VisitAtom(at AtomType, lexeme string) (value.Value, error)
	VisitBinary(left Ast, op string, right Ast) (value.Value, error)
	VisitList(values []Ast) (value.Value, error)
	VisitDict(values map[string]Ast) (value.Value, error)
	VisitTable(inner Ast) (value.Value, error)
	VisitOpcall(operand Ast, namespace, name string, kwargs Dict) (value.Value, error)
	VisitVar(name string) (value.Value, error)
	VisitStatement(name string, body Ast) (value.Value, error)
	VisitQuery(statements []Statement) (value.Value, error)
	VisitAt() (value.Value, error)
	VisitLookup(on Ast, property string) (value.Value, error)
	VisitIfelse(condition Ast, thenDo Ast, elseDo Ast) (value.Value, error)
}

type Ast interface {
	AcceptValue(v VisitorValue) (value.Value, error)
	AcceptString(v VisitorString) string
	Equals(ast Ast) bool
	toProto() (proto.Ast, error)
}

var _ Ast = Atom{}
var _ Ast = Binary{}
var _ Ast = List{}
var _ Ast = Table{}
var _ Ast = OpCall{}
var _ Ast = Var{}
var _ Ast = Statement{}
var _ Ast = Query{}
var _ Ast = At{}
var _ Ast = Lookup{}
var _ Ast = IfElse{}

type Lookup struct {
	On       Ast
	Property string
}

func (l Lookup) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitLookup(l.On, l.Property)
}

func (l Lookup) AcceptString(v VisitorString) string {
	return v.VisitLookup(l.On, l.Property)
}

func (l Lookup) Equals(ast Ast) bool {
	switch l2 := ast.(type) {
	case Lookup:
		return l.On.Equals(l2.On) && l.Property == l2.Property
	default:
		return false
	}
}

type At struct{}

func (a At) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitAt()
}

func (a At) AcceptString(v VisitorString) string {
	return v.VisitAt()
}

func (a At) Equals(ast Ast) bool {
	switch ast.(type) {
	case At:
		return true
	default:
		return false
	}
}

type Statement struct {
	Name string
	Body Ast
}

func (s Statement) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitStatement(s.Name, s.Body)
}

func (s Statement) AcceptString(v VisitorString) string {
	return v.VisitStatement(s.Name, s.Body)
}

func (s Statement) Equals(ast Ast) bool {
	switch s2 := ast.(type) {
	case Statement:
		return s.Name == s2.Name && s.Body.Equals(s2.Body)
	default:
		return false
	}
}

type Query struct {
	Statements []Statement
}

func (q Query) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitQuery(q.Statements)
}

func (q Query) AcceptString(v VisitorString) string {
	return v.VisitQuery(q.Statements)
}

func (q Query) Equals(ast Ast) bool {
	switch q2 := ast.(type) {
	case Query:
		if len(q.Statements) != len(q2.Statements) {
			return false
		}
		for i, s := range q.Statements {
			if !s.Equals(q2.Statements[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

type AtomType uint8

const (
	Int AtomType = 1 + iota
	String
	Bool
	Double
)

type Atom struct {
	Type   AtomType
	Lexeme string
}

func (a Atom) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitAtom(a.Type, a.Lexeme)
}

func (a Atom) AcceptString(v VisitorString) string {
	return v.VisitAtom(a.Type, a.Lexeme)
}

func (a Atom) Equals(ast Ast) bool {
	switch a2 := ast.(type) {
	case Atom:
		return a.Type == a2.Type && a.Lexeme == a2.Lexeme
	default:
		return false
	}
}

type Binary struct {
	Left  Ast
	Op    string
	Right Ast
}

func (b Binary) AcceptString(v VisitorString) string {
	return v.VisitBinary(b.Left, b.Op, b.Right)
}

func (b Binary) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitBinary(b.Left, b.Op, b.Right)
}

func (b Binary) Equals(ast Ast) bool {
	switch b2 := ast.(type) {
	case Binary:
		return b.Left.Equals(b2.Left) && b.Op == b2.Op && b.Right.Equals(b2.Right)
	default:
		return false
	}
}

type List struct {
	Values []Ast
}

func (l List) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitList(l.Values)
}

func (l List) AcceptString(v VisitorString) string {
	return v.VisitList(l.Values)
}

func (l List) Equals(ast Ast) bool {
	switch l2 := ast.(type) {
	case List:
		if len(l.Values) != len(l2.Values) {
			return false
		}
		for i, v := range l.Values {
			if !v.Equals(l2.Values[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

type Dict struct {
	Values map[string]Ast
}

func (d Dict) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitDict(d.Values)
}

func (d Dict) AcceptString(v VisitorString) string {
	return v.VisitDict(d.Values)
}

func (d Dict) Equals(ast Ast) bool {
	switch d2 := ast.(type) {
	case Dict:
		if len(d.Values) != len(d2.Values) {
			return false
		}
		for k, v := range d.Values {
			if v2, ok := d2.Values[k]; !(ok && v.Equals(v2)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

type Table struct {
	Inner Ast
}

func (t Table) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitTable(t.Inner)
}

func (t Table) AcceptString(v VisitorString) string {
	return v.VisitTable(t.Inner)
}

func (t Table) Equals(ast Ast) bool {
	switch t2 := ast.(type) {
	case Table:
		return t.Inner.Equals(t2.Inner)
	default:
		return false
	}
}

type OpCall struct {
	Operand   Ast
	Namespace string
	Name      string
	Kwargs    Dict
}

func (opcall OpCall) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitOpcall(opcall.Operand, opcall.Namespace, opcall.Name, opcall.Kwargs)
}

func (opcall OpCall) AcceptString(v VisitorString) string {
	return v.VisitOpcall(opcall.Operand, opcall.Namespace, opcall.Name, opcall.Kwargs)
}

func (opcall OpCall) Equals(ast Ast) bool {
	switch opcall2 := ast.(type) {
	case OpCall:
		return opcall.Operand.Equals(opcall2.Operand) &&
			opcall.Namespace == opcall2.Namespace &&
			opcall.Name == opcall2.Name &&
			opcall.Kwargs.Equals(opcall2.Kwargs)
	default:
		return false
	}
}

type Var struct {
	Name string
}

func (va Var) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitVar(va.Name)
}

func (va Var) AcceptString(v VisitorString) string {
	return v.VisitVar(va.Name)
}

func (va Var) Equals(ast Ast) bool {
	switch va2 := ast.(type) {
	case Var:
		return va.Name == va2.Name
	default:
		return false
	}
}

type IfElse struct {
	Condition Ast
	ThenDo    Ast
	ElseDo    Ast
}

func (ifelse IfElse) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitIfelse(ifelse.Condition, ifelse.ThenDo, ifelse.ElseDo)
}

func (ifelse IfElse) AcceptString(v VisitorString) string {
	return v.VisitIfelse(ifelse.Condition, ifelse.ThenDo, ifelse.ElseDo)
}

func (ifelse IfElse) Equals(ast Ast) bool {
	switch ifelse2 := ast.(type) {
	case IfElse:
		return ifelse.Condition.Equals(ifelse2.Condition) &&
			ifelse.ThenDo.Equals(ifelse2.ThenDo) &&
			ifelse.ElseDo.Equals(ifelse2.ElseDo)
	default:
		return false
	}
}
