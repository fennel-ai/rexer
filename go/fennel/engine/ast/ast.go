package ast

import (
	"fennel/engine/ast/proto"
	"fennel/lib/value"
)

type VisitorString interface {
	VisitAtom(at AtomType, lexeme string) string
	VisitUnary(op string, operand Ast) string
	VisitBinary(left Ast, op string, right Ast) string
	VisitList(values []Ast) string
	VisitDict(values map[string]Ast) string
	VisitOpcall(operands []Ast, vars []string, namespace, name string, kwargs *Dict) string
	VisitVar(name string) string
	VisitStatement(name string, body Ast) string
	VisitQuery(statements []*Statement) string
	VisitLookup(on Ast, property string) string
	VisitIfelse(condition Ast, thenDo Ast, elseDo Ast) string
}

type VisitorValue interface {
	VisitAtom(at AtomType, lexeme string) (value.Value, error)
	VisitUnary(op string, operand Ast) (value.Value, error)
	VisitBinary(left Ast, op string, right Ast) (value.Value, error)
	VisitList(values []Ast) (value.Value, error)
	VisitDict(values map[string]Ast) (value.Value, error)
	VisitOpcall(operand []Ast, vars []string, namespace, name string, kwargs *Dict) (value.Value, error)
	VisitVar(name string) (value.Value, error)
	VisitStatement(name string, body Ast) (value.Value, error)
	VisitQuery(statements []*Statement) (value.Value, error)
	VisitLookup(on Ast, property string) (value.Value, error)
	VisitIfelse(condition Ast, thenDo Ast, elseDo Ast) (value.Value, error)
}

type Ast interface {
	AcceptValue(v VisitorValue) (value.Value, error)
	AcceptString(v VisitorString) string
	Equals(ast Ast) bool
	toProto() (proto.Ast, error)
}

var _ Ast = (*Atom)(nil)
var _ Ast = (*Unary)(nil)
var _ Ast = (*Binary)(nil)
var _ Ast = (*List)(nil)
var _ Ast = (*Dict)(nil)
var _ Ast = (*OpCall)(nil)
var _ Ast = (*Var)(nil)
var _ Ast = (*Statement)(nil)
var _ Ast = (*Query)(nil)
var _ Ast = (*Lookup)(nil)
var _ Ast = (*IfElse)(nil)

type Lookup struct {
	On       Ast
	Property string
}

func (l *Lookup) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitLookup(l.On, l.Property)
}

func (l *Lookup) AcceptString(v VisitorString) string {
	return v.VisitLookup(l.On, l.Property)
}

func (l *Lookup) Equals(ast Ast) bool {
	switch l2 := ast.(type) {
	case *Lookup:
		return l.On.Equals(l2.On) && l.Property == l2.Property
	default:
		return false
	}
}

type Statement struct {
	Name string
	Body Ast
}

func (s *Statement) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitStatement(s.Name, s.Body)
}

func (s *Statement) AcceptString(v VisitorString) string {
	return v.VisitStatement(s.Name, s.Body)
}

func (s *Statement) Equals(ast Ast) bool {
	switch s2 := ast.(type) {
	case *Statement:
		return s.Name == s2.Name && s.Body.Equals(s2.Body)
	default:
		return false
	}
}

type Query struct {
	Statements []*Statement
}

func (q *Query) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitQuery(q.Statements)
}

func (q *Query) AcceptString(v VisitorString) string {
	return v.VisitQuery(q.Statements)
}

func (q Query) Equals(ast Ast) bool {
	switch q2 := ast.(type) {
	case *Query:
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

func (a *Atom) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitAtom(a.Type, a.Lexeme)
}

func (a *Atom) AcceptString(v VisitorString) string {
	return v.VisitAtom(a.Type, a.Lexeme)
}

func (a *Atom) Equals(ast Ast) bool {
	switch a2 := ast.(type) {
	case *Atom:
		return a.Type == a2.Type && a.Lexeme == a2.Lexeme
	default:
		return false
	}
}

type Unary struct {
	Op      string
	Operand Ast
}

func (u *Unary) AcceptString(v VisitorString) string {
	return v.VisitUnary(u.Op, u.Operand)
}

func (u *Unary) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitUnary(u.Op, u.Operand)
}

func (u *Unary) Equals(ast Ast) bool {
	switch u2 := ast.(type) {
	case *Unary:
		return u.Op == u2.Op && u.Operand.Equals(u2.Operand)
	default:
		return false
	}
}

type Binary struct {
	Left  Ast
	Op    string
	Right Ast
}

func (b *Binary) AcceptString(v VisitorString) string {
	return v.VisitBinary(b.Left, b.Op, b.Right)
}

func (b *Binary) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitBinary(b.Left, b.Op, b.Right)
}

func (b *Binary) Equals(ast Ast) bool {
	switch b2 := ast.(type) {
	case *Binary:
		return b.Left.Equals(b2.Left) && b.Op == b2.Op && b.Right.Equals(b2.Right)
	default:
		return false
	}
}

type List struct {
	Values []Ast
}

func (l *List) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitList(l.Values)
}

func (l *List) AcceptString(v VisitorString) string {
	return v.VisitList(l.Values)
}

func (l *List) Equals(ast Ast) bool {
	switch l2 := ast.(type) {
	case *List:
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

func (d *Dict) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitDict(d.Values)
}

func (d *Dict) AcceptString(v VisitorString) string {
	return v.VisitDict(d.Values)
}

func (d *Dict) Equals(ast Ast) bool {
	switch d2 := ast.(type) {
	case *Dict:
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

type OpCall struct {
	Namespace string
	Name      string
	Operands  []Ast
	Vars      []string
	Kwargs    *Dict
}

func (opcall *OpCall) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitOpcall(opcall.Operands, opcall.Vars, opcall.Namespace, opcall.Name, opcall.Kwargs)
}

func (opcall *OpCall) AcceptString(v VisitorString) string {
	return v.VisitOpcall(opcall.Operands, opcall.Vars, opcall.Namespace, opcall.Name, opcall.Kwargs)
}

func (opcall *OpCall) Equals(ast Ast) bool {
	switch opcall2 := ast.(type) {
	case *OpCall:
		if len(opcall.Vars) != len(opcall2.Vars) {
			return false
		}
		for i := range opcall.Vars {
			if opcall.Vars[i] != opcall2.Vars[i] {
				return false
			}
		}
		l1 := &List{Values: opcall.Operands}
		l2 := &List{Values: opcall2.Operands}
		return l1.Equals(l2) &&
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

func (va *Var) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitVar(va.Name)
}

func (va *Var) AcceptString(v VisitorString) string {
	return v.VisitVar(va.Name)
}

func (va *Var) Equals(ast Ast) bool {
	switch va2 := ast.(type) {
	case *Var:
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

func (ifelse *IfElse) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitIfelse(ifelse.Condition, ifelse.ThenDo, ifelse.ElseDo)
}

func (ifelse *IfElse) AcceptString(v VisitorString) string {
	return v.VisitIfelse(ifelse.Condition, ifelse.ThenDo, ifelse.ElseDo)
}

func (ifelse *IfElse) Equals(ast Ast) bool {
	switch ifelse2 := ast.(type) {
	case *IfElse:
		return ifelse.Condition.Equals(ifelse2.Condition) &&
			ifelse.ThenDo.Equals(ifelse2.ThenDo) &&
			ifelse.ElseDo.Equals(ifelse2.ElseDo)
	default:
		return false
	}
}
