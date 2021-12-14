package ast

import "engine/runtime"

// TODO: make this generic instead of string
type VisitorValue interface {
	VisitAtom(at AtomType, lexeme string) (runtime.Value, error)
	VisitBinary(left Ast, op string, right Ast) (runtime.Value, error)
	VisitList(values []Ast) (runtime.Value, error)
	VisitDict(values map[string]Ast) (runtime.Value, error)
	VisitTable(inner Ast) (runtime.Value, error)
	VisitOpcall(operand Ast, namespace, name string, kwargs Dict) (runtime.Value, error)
	VisitVar(name string) (runtime.Value, error)
	VisitStatement(name string, body Ast) (runtime.Value, error)
	VisitQuery(statements []Statement) (runtime.Value, error)
}
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
type Ast interface {
	AcceptValue(v VisitorValue) (runtime.Value, error)
	AcceptString(v VisitorString) string
}

var _ Ast = Atom{}
var _ Ast = Binary{}
var _ Ast = List{}
var _ Ast = Dict{}
var _ Ast = Table{}
var _ Ast = OpCall{}
var _ Ast = Var{}
var _ Ast = Statement{}
var _ Ast = Query{}

type Statement struct {
	name string
	body Ast
}

func (s Statement) AcceptValue(v VisitorValue) (runtime.Value, error) {
	return v.VisitStatement(s.name, s.body)
}

func (s Statement) AcceptString(v VisitorString) string {
	return v.VisitStatement(s.name, s.body)
}

type Query struct {
	statements []Statement
}

func (q Query) AcceptValue(v VisitorValue) (runtime.Value, error) {
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
	lexeme string
}

func (a Atom) AcceptValue(v VisitorValue) (runtime.Value, error) {
	return v.VisitAtom(a.Type, a.lexeme)
}

func (a Atom) AcceptString(v VisitorString) string {
	return v.VisitAtom(a.Type, a.lexeme)
}

type Binary struct {
	left  Ast
	op    string
	right Ast
}

func (b Binary) AcceptString(v VisitorString) string {
	return v.VisitBinary(b.left, b.op, b.right)
}

func (b Binary) AcceptValue(v VisitorValue) (runtime.Value, error) {
	return v.VisitBinary(b.left, b.op, b.right)
}

type List struct {
	values []Ast
}

func (l List) AcceptValue(v VisitorValue) (runtime.Value, error) {
	return v.VisitList(l.values)
}

func (l List) AcceptString(v VisitorString) string {
	return v.VisitList(l.values)
}

type Dict struct {
	values map[string]Ast
}

func (d Dict) AcceptValue(v VisitorValue) (runtime.Value, error) {
	return v.VisitDict(d.values)
}

func (d Dict) AcceptString(v VisitorString) string {
	return v.VisitDict(d.values)
}

type Table struct {
	inner Ast
}

func (t Table) AcceptValue(v VisitorValue) (runtime.Value, error) {
	return v.VisitTable(t.inner)
}

func (t Table) AcceptString(v VisitorString) string {
	return v.VisitTable(t.inner)
}

type OpCall struct {
	operand   Ast
	namespace string
	name      string
	kwargs    Dict
}

func (opcall OpCall) AcceptValue(v VisitorValue) (runtime.Value, error) {
	return v.VisitOpcall(opcall.operand, opcall.namespace, opcall.name, opcall.kwargs)
}

func (opcall OpCall) AcceptString(v VisitorString) string {
	return v.VisitOpcall(opcall.operand, opcall.namespace, opcall.name, opcall.kwargs)
}

type Var struct {
	name string
}

func (va Var) AcceptValue(v VisitorValue) (runtime.Value, error) {
	return v.VisitVar(va.name)
}

func (va Var) AcceptString(v VisitorString) string {
	return v.VisitVar(va.name)
}
