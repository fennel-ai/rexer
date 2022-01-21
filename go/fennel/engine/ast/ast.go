package ast

import (
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
}

type Ast interface {
	AcceptValue(v VisitorValue) (value.Value, error)
	AcceptString(v VisitorString) string
}

var _ Ast = Atom{}
var _ Ast = Binary{}
var _ Ast = List{}
var _ Ast = Table{}
var _ Ast = OpCall{}
var _ Ast = Var{}
var _ Ast = Statement{}
var _ Ast = Query{}

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

type Query struct {
	statements []Statement
}

func (q Query) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitQuery(q.statements)
}

func (q Query) AcceptString(v VisitorString) string {
	return v.VisitQuery(q.statements)
}

type AtomType uint8

const (
	Int    AtomType = 1
	String          = 2
	Bool            = 3
	Double          = 4
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

type List struct {
	Values []Ast
}

func (l List) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitList(l.Values)
}

func (l List) AcceptString(v VisitorString) string {
	return v.VisitList(l.Values)
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

type Table struct {
	Inner Ast
}

func (t Table) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitTable(t.Inner)
}

func (t Table) AcceptString(v VisitorString) string {
	return v.VisitTable(t.Inner)
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

type Var struct {
	name string
}

func (va Var) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitVar(va.name)
}

func (va Var) AcceptString(v VisitorString) string {
	return v.VisitVar(va.name)
}
