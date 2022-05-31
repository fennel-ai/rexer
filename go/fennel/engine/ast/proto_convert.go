package ast

import (
	"fmt"
	"strconv"

	"fennel/engine/ast/proto"
)

func ToProtoAst(ast Ast) (proto.Ast, error) {
	if ast == nil {
		return pnull(), fmt.Errorf("can not convert nil ast to proto")
	}
	return ast.toProto()
}

func FromProtoAst(past *proto.Ast) (Ast, error) {
	switch n := past.Node.(type) {
	case *proto.Ast_Atom:
		return fromProtoAtom(n)
	case *proto.Ast_Unary:
		return fromProtoUnary(n)
	case *proto.Ast_Binary:
		return fromProtoBinary(n)
	case *proto.Ast_List:
		return fromProtoList(n)
	case *proto.Ast_Dict:
		return fromProtoDict(n)
	case *proto.Ast_Statement:
		return fromProtoStatement(n)
	case *proto.Ast_Query:
		return fromProtoQuery(n)
	case *proto.Ast_Opcall:
		return fromProtoOpcall(n)
	case *proto.Ast_Var:
		return fromProtoVar(n)
	case *proto.Ast_Lookup:
		return fromProtoLookup(n)
	case *proto.Ast_Ifelse:
		return fromProtoIfelse(n)
	default:
		return null, fmt.Errorf("invalid proto ast: %v", past)
	}
}

// =====================================
// Satisfy ast interface requirements
// =====================================

func (l *Lookup) toProto() (proto.Ast, error) {
	pon, err := ToProtoAst(l.On)
	if err != nil {
		return pnull(), err
	}
	return proto.Ast{Node: &proto.Ast_Lookup{Lookup: &proto.Lookup{
		On:       &pon,
		Property: l.Property,
	}}}, nil
}

func (a *Atom) toProto() (proto.Ast, error) {
	switch a.Type {
	case Int:
		n, err := strconv.ParseInt(a.Lexeme, 10, 64)
		if err == nil {
			return proto.Ast{Node: &proto.Ast_Atom{Atom: &proto.Atom{Inner: &proto.Atom_Int{Int: n}}}}, nil
		} else {
			return pnull(), err
		}
	case Double:
		d, err := strconv.ParseFloat(a.Lexeme, 64)
		if err == nil {
			return proto.Ast{Node: &proto.Ast_Atom{Atom: &proto.Atom{Inner: &proto.Atom_Double{Double: d}}}}, nil
		} else {
			return pnull(), err
		}

	case Bool:
		b, err := strconv.ParseBool(a.Lexeme)
		if err == nil {
			return proto.Ast{Node: &proto.Ast_Atom{Atom: &proto.Atom{Inner: &proto.Atom_Bool{Bool: b}}}}, nil
		} else {
			return pnull(), err
		}
	case String:
		return proto.Ast{Node: &proto.Ast_Atom{Atom: &proto.Atom{Inner: &proto.Atom_String_{String_: a.Lexeme}}}}, nil
	default:
		return pnull(), fmt.Errorf("invalid atom type: %v", a.Type)
	}

}

func (u *Unary) toProto() (proto.Ast, error) {
	protoOperand, err := ToProtoAst(u.Operand)
	if err != nil {
		return pnull(), err
	}
	return proto.Ast{Node: &proto.Ast_Unary{Unary: &proto.Unary{
		Op:      u.Op,
		Operand: &protoOperand,
	}}}, nil
}

func (b *Binary) toProto() (proto.Ast, error) {
	protoLeft, err := ToProtoAst(b.Left)
	if err != nil {
		return pnull(), err
	}
	protoRight, err := ToProtoAst(b.Right)
	if err != nil {
		return pnull(), err
	}
	return proto.Ast{Node: &proto.Ast_Binary{Binary: &proto.Binary{
		Left:  &protoLeft,
		Right: &protoRight,
		Op:    b.Op,
	}}}, nil
}

func (l *List) toProto() (proto.Ast, error) {
	ret := make([]*proto.Ast, len(l.Values))
	for i, ast := range l.Values {
		past, err := ToProtoAst(ast)
		if err != nil {
			return pnull(), err
		}
		ret[i] = &past
	}
	return proto.Ast{Node: &proto.Ast_List{List: &proto.List{Values: ret}}}, nil
}

func (s *Statement) toProto() (proto.Ast, error) {
	pbody, err := ToProtoAst(s.Body)
	if err != nil {
		return pnull(), err
	}
	return proto.Ast{Node: &proto.Ast_Statement{Statement: &proto.Statement{
		Name: s.Name,
		Body: &pbody,
	}}}, nil
}

func (q *Query) toProto() (proto.Ast, error) {
	ret := make([]*proto.Statement, len(q.Statements))
	for i, s := range q.Statements {
		ps, err := ToProtoAst(s)
		if err != nil {
			return pnull(), err
		}
		ret[i] = ps.GetStatement()
	}
	return proto.Ast{Node: &proto.Ast_Query{Query: &proto.Query{Statements: ret}}}, nil
}

func (d *Dict) toProto() (proto.Ast, error) {
	ret := make(map[string]*proto.Ast, len(d.Values))
	for k, ast := range d.Values {
		past, err := ToProtoAst(ast)
		if err != nil {
			return pnull(), err
		}
		ret[k] = &past
	}
	return proto.Ast{Node: &proto.Ast_Dict{Dict: &proto.Dict{Values: ret}}}, nil
}

func (opcall *OpCall) toProto() (proto.Ast, error) {
	poperands := make([]*proto.Ast, len(opcall.Operands))
	for i, operand := range opcall.Operands {
		poperand, err := ToProtoAst(operand)
		if err != nil {
			return pnull(), err
		}
		poperands[i] = &poperand
	}

	pdict, err := ToProtoAst(opcall.Kwargs)
	if err != nil {
		return pnull(), err
	}
	return proto.Ast{Node: &proto.Ast_Opcall{Opcall: &proto.OpCall{
		Operands:  poperands,
		Vars:      opcall.Vars,
		Namespace: opcall.Namespace,
		Name:      opcall.Name,
		Kwargs:    pdict.GetDict(),
	}}}, nil
}

func (va *Var) toProto() (proto.Ast, error) {
	return proto.Ast{Node: &proto.Ast_Var{Var: &proto.Var{Name: va.Name}}}, nil
}

func (ifelse *IfElse) toProto() (proto.Ast, error) {
	protoCondition, err := ToProtoAst(ifelse.Condition)
	if err != nil {
		return pnull(), err
	}
	protoThenDo, err := ToProtoAst(ifelse.ThenDo)
	if err != nil {
		return pnull(), err
	}
	protoElseDo, err := ToProtoAst(ifelse.ElseDo)
	if err != nil {
		return pnull(), err
	}
	return proto.Ast{Node: &proto.Ast_Ifelse{Ifelse: &proto.IfElse{
		Condition: &protoCondition,
		ThenDo:    &protoThenDo,
		ElseDo:    &protoElseDo,
	}}}, nil
}

// =============================
// More private helpers below
// =============================

var null = &Atom{}

func pnull() proto.Ast {
	return proto.Ast{}
}

func fromProtoLookup(plookup *proto.Ast_Lookup) (Ast, error) {
	on, err := FromProtoAst(plookup.Lookup.On)
	if err != nil {
		return null, err
	}
	return &Lookup{On: on, Property: plookup.Lookup.Property}, nil
}

func fromProtoVar(pvar *proto.Ast_Var) (Ast, error) {
	return &Var{Name: pvar.Var.Name}, nil
}

func fromProtoOpcall(popcall *proto.Ast_Opcall) (Ast, error) {
	ret := &OpCall{
		Name:      popcall.Opcall.Name,
		Namespace: popcall.Opcall.Namespace,
		Vars:      popcall.Opcall.Vars,
	}
	for i := range popcall.Opcall.Operands {
		operand, err := FromProtoAst(popcall.Opcall.Operands[i])
		if err != nil {
			return null, err
		}
		ret.Operands = append(ret.Operands, operand)
	}
	dict, err := FromProtoAst(&proto.Ast{Node: &proto.Ast_Dict{Dict: popcall.Opcall.Kwargs}})
	if err != nil {
		return null, err
	}
	ret.Kwargs = dict.(*Dict)
	return ret, nil
}

func fromProtoQuery(pquery *proto.Ast_Query) (Ast, error) {
	statements := make([]*Statement, len(pquery.Query.Statements))
	for i, ps := range pquery.Query.Statements {
		s, err := FromProtoAst(&proto.Ast{Node: &proto.Ast_Statement{Statement: ps}})
		if err != nil {
			return null, err
		}
		statements[i] = s.(*Statement)
	}
	return &Query{Statements: statements}, nil
}

func fromProtoStatement(pstatement *proto.Ast_Statement) (Ast, error) {
	body, err := FromProtoAst(pstatement.Statement.Body)
	if err != nil {
		return null, err
	}
	return &Statement{
		Name: pstatement.Statement.Name,
		Body: body,
	}, nil
}

func fromProtoAtom(patom *proto.Ast_Atom) (Ast, error) {
	if pint, ok := patom.Atom.Inner.(*proto.Atom_Int); ok {
		return &Atom{Type: Int, Lexeme: fmt.Sprintf("%d", pint.Int)}, nil
	}
	if pdouble, ok := patom.Atom.Inner.(*proto.Atom_Double); ok {
		// when printing back to float, don't add trailing zeros
		return &Atom{Type: Double, Lexeme: strconv.FormatFloat(pdouble.Double, 'f', -1, 64)}, nil
	}
	if pbool, ok := patom.Atom.Inner.(*proto.Atom_Bool); ok {
		var str string
		if pbool.Bool {
			str = "true"
		} else {
			str = "false"
		}
		return &Atom{Type: Bool, Lexeme: str}, nil
	}
	if pstr, ok := patom.Atom.Inner.(*proto.Atom_String_); ok {
		return &Atom{Type: String, Lexeme: pstr.String_}, nil
	}
	return null, fmt.Errorf("invalid proto atom: %v", patom)
}

func fromProtoList(plist *proto.Ast_List) (Ast, error) {
	pvalues := plist.List.Values
	values := make([]Ast, len(pvalues))
	for i, pv := range pvalues {
		v, err := FromProtoAst(pv)
		if err != nil {
			return null, err
		}
		values[i] = v
	}
	return MakeList(values...), nil
}

func fromProtoDict(plist *proto.Ast_Dict) (Ast, error) {
	pvalues := plist.Dict.Values
	values := make(map[string]Ast, len(pvalues))
	for i, pv := range pvalues {
		v, err := FromProtoAst(pv)
		if err != nil {
			return null, err
		}
		values[i] = v
	}
	return MakeDict(values), nil
}

func fromProtoUnary(pun *proto.Ast_Unary) (Ast, error) {
	operand, err := FromProtoAst(pun.Unary.Operand)
	if err != nil {
		return nil, err
	}
	return &Unary{
		Op:      pun.Unary.Op,
		Operand: operand,
	}, nil
}

func fromProtoBinary(pbin *proto.Ast_Binary) (Ast, error) {
	left, err := FromProtoAst(pbin.Binary.Left)
	if err != nil {
		return null, err
	}
	right, err := FromProtoAst(pbin.Binary.Right)
	if err != nil {
		return null, err
	}
	return &Binary{
		Left:  left,
		Op:    pbin.Binary.Op,
		Right: right,
	}, nil
}

func fromProtoIfelse(pifelse *proto.Ast_Ifelse) (Ast, error) {
	condition, err := FromProtoAst(pifelse.Ifelse.Condition)
	if err != nil {
		return null, err
	}
	thenDo, err := FromProtoAst(pifelse.Ifelse.ThenDo)
	if err != nil {
		return null, err
	}
	elseDo, err := FromProtoAst(pifelse.Ifelse.ElseDo)
	if err != nil {
		return null, err
	}
	return &IfElse{
		Condition: condition,
		ThenDo:    thenDo,
		ElseDo:    elseDo,
	}, nil
}
