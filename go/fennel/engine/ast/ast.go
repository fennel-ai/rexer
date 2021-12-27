package ast

import (
	"fennel/value"
	"fmt"
)

// TODO: make this generic instead of string
type VisitorString interface {
	VisitAtom(at AtomType, lexeme string) string
	VisitBinary(left *Ast, op string, right *Ast) string
	VisitList(values []*Ast) string
	VisitDict(values map[string]*Ast) string
	VisitTable(inner *Ast) string
	VisitOpcall(operand *Ast, namespace, name string, kwargs *Dict) string
	VisitVar(name string) string
	VisitStatement(name string, body *Ast) string
	VisitQuery(statements []*Statement) string
}

type VisitorValue interface {
	VisitAtom(at AtomType, lexeme string) (value.Value, error)
	VisitBinary(left *Ast, op string, right *Ast) (value.Value, error)
	VisitList(values []*Ast) (value.Value, error)
	VisitDict(values map[string]*Ast) (value.Value, error)
	VisitTable(inner *Ast) (value.Value, error)
	VisitOpcall(operand *Ast, namespace, name string, kwargs *Dict) (value.Value, error)
	VisitVar(name string) (value.Value, error)
	VisitStatement(name string, body *Ast) (value.Value, error)
	VisitQuery(statements []*Statement) (value.Value, error)
}

type AstNode interface {
	AcceptValue(v VisitorValue) (value.Value, error)
	AcceptString(v VisitorString) string
}

var _ AstNode = (*Ast)(nil)
var _ AstNode = (*Atom)(nil)
var _ AstNode = (*Binary)(nil)
var _ AstNode = (*List)(nil)
var _ AstNode = (*Dict)(nil)
var _ AstNode = (*Table)(nil)
var _ AstNode = (*OpCall)(nil)
var _ AstNode = (*Var)(nil)
var _ AstNode = (*Statement)(nil)
var _ AstNode = (*Query)(nil)

func (a *Atom) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitAtom(a.AtomType, a.Lexeme)
}

func (a *Atom) AcceptString(v VisitorString) string {
	return v.VisitAtom(a.AtomType, a.Lexeme)
}

func (b *Binary) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitBinary(b.Left, b.Op, b.Right)
}

func (b *Binary) AcceptString(v VisitorString) string {
	return v.VisitBinary(b.Left, b.Op, b.Right)
}

func (d *Dict) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitDict(d.Values)
}

func (d *Dict) AcceptString(v VisitorString) string {
	return v.VisitDict(d.Values)
}

func (l *List) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitList(l.Elems)
}

func (l *List) AcceptString(v VisitorString) string {
	return v.VisitList(l.Elems)
}

func (o *OpCall) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitOpcall(o.Operand, o.Namespace, o.Name, o.Kwargs)
}

func (o *OpCall) AcceptString(v VisitorString) string {
	return v.VisitOpcall(o.Operand, o.Namespace, o.Name, o.Kwargs)
}

func (r *Var) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitVar(r.Name)
}

func (r *Var) AcceptString(v VisitorString) string {
	return v.VisitVar(r.Name)
}

func (t *Table) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitTable(t.Inner)
}

func (t *Table) AcceptString(v VisitorString) string {
	return v.VisitTable(t.Inner)
}

func (s *Statement) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitStatement(s.Name, s.Body)
}

func (s *Statement) AcceptString(v VisitorString) string {
	return v.VisitStatement(s.Name, s.Body)
}

func (q *Query) AcceptValue(v VisitorValue) (value.Value, error) {
	return v.VisitQuery(q.Statements)
}

func (q *Query) AcceptString(v VisitorString) string {
	return v.VisitQuery(q.Statements)
}

func (a *Ast) AcceptValue(v VisitorValue) (value.Value, error) {
	node := a.Node
	switch node.(type) {
	case *Ast_Atom:
		return node.(*Ast_Atom).Atom.AcceptValue(v)
	case *Ast_Binary:
		return node.(*Ast_Binary).Binary.AcceptValue(v)
	case *Ast_List:
		return node.(*Ast_List).List.AcceptValue(v)
	case *Ast_Dict:
		return node.(*Ast_Dict).Dict.AcceptValue(v)
	case *Ast_Table:
		return node.(*Ast_Table).Table.AcceptValue(v)
	case *Ast_Var:
		return node.(*Ast_Var).Var.AcceptValue(v)
	case *Ast_Opcall:
		return node.(*Ast_Opcall).Opcall.AcceptValue(v)
	case *Ast_Statement:
		return node.(*Ast_Statement).Statement.AcceptValue(v)
	case *Ast_Query:
		return node.(*Ast_Query).Query.AcceptValue(v)
	}
	panic(fmt.Sprintf("unexpected node type: %v", node))
}

func (a *Ast) AcceptString(v VisitorString) string {
	node := a.Node
	switch node.(type) {
	case *Ast_Atom:
		return node.(*Ast_Atom).Atom.AcceptString(v)
	case *Ast_Binary:
		return node.(*Ast_Binary).Binary.AcceptString(v)
	case *Ast_List:
		return node.(*Ast_List).List.AcceptString(v)
	case *Ast_Dict:
		return node.(*Ast_Dict).Dict.AcceptString(v)
	case *Ast_Table:
		return node.(*Ast_Table).Table.AcceptString(v)
	case *Ast_Var:
		return node.(*Ast_Var).Var.AcceptString(v)
	case *Ast_Opcall:
		return node.(*Ast_Opcall).Opcall.AcceptString(v)
	case *Ast_Statement:
		return node.(*Ast_Statement).Statement.AcceptString(v)
	case *Ast_Query:
		return node.(*Ast_Query).Query.AcceptString(v)
	}
	panic(fmt.Sprintf("unexpected node type: %v", node))
}

func MakeAst(node AstNode) *Ast {
	switch node.(type) {
	case *Atom:
		return &Ast{Node: &Ast_Atom{Atom: node.(*Atom)}}
	case *Binary:
		return &Ast{Node: &Ast_Binary{Binary: node.(*Binary)}}
	case *List:
		return &Ast{Node: &Ast_List{List: node.(*List)}}
	case *Dict:
		return &Ast{Node: &Ast_Dict{Dict: node.(*Dict)}}
	case *Table:
		return &Ast{Node: &Ast_Table{Table: node.(*Table)}}
	case *Var:
		return &Ast{Node: &Ast_Var{Var: node.(*Var)}}
	case *OpCall:
		return &Ast{Node: &Ast_Opcall{Opcall: node.(*OpCall)}}
	case *Statement:
		return &Ast{Node: &Ast_Statement{Statement: node.(*Statement)}}
	case *Query:
		return &Ast{Node: &Ast_Query{Query: node.(*Query)}}
	default:
		panic(fmt.Sprintf("unexpexted node: %v", node))
	}
}
