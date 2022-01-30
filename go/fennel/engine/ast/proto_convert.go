package ast

import (
	"fennel/engine/ast/proto"
	"fmt"
	"strconv"
)

func ToProtoAst(ast Ast) (proto.Ast, error) {
	switch ast.(type) {
	case Atom:
		return ast.(Atom).toProto()
	case Binary:
		return ast.(Binary).toProto()
	case Var:
		return ast.(Var).toProto()
	case List:
		return ast.(List).toProto()
	case Dict:
		return ast.(Dict).toProto()
	case Statement:
		return ast.(Statement).toProto()
	case Query:
		return ast.(Query).toProto()
	case OpCall:
		return ast.(OpCall).toProto()
	case Table:
		return ast.(Table).toProto()
	case Lookup:
		return ast.(Lookup).toProto()
	case At:
		return ast.(At).toProto()
	case IfElse:
		return ast.(IfElse).toProto()
	default:
		return pnull, fmt.Errorf("invalid ast type")
	}
}

func FromProtoAst(past proto.Ast) (Ast, error) {
	switch past.Node.(type) {
	case *proto.Ast_Atom:
		return fromProtoAtom(past.Node.(*proto.Ast_Atom))
	case *proto.Ast_Binary:
		return fromProtoBinary(past.Node.(*proto.Ast_Binary))
	case *proto.Ast_List:
		return fromProtoList(past.Node.(*proto.Ast_List))
	case *proto.Ast_Dict:
		return fromProtoDict(past.Node.(*proto.Ast_Dict))
	case *proto.Ast_Statement:
		return fromProtoStatement(past.Node.(*proto.Ast_Statement))
	case *proto.Ast_Query:
		return fromProtoQuery(past.Node.(*proto.Ast_Query))
	case *proto.Ast_Opcall:
		return fromProtoOpcall(past.Node.(*proto.Ast_Opcall))
	case *proto.Ast_Var:
		return fromProtoVar(past.Node.(*proto.Ast_Var))
	case *proto.Ast_Table:
		return fromProtoTable(past.Node.(*proto.Ast_Table))
	case *proto.Ast_At:
		return fromProtoAt(past.Node.(*proto.Ast_At))
	case *proto.Ast_Lookup:
		return fromProtoLookup(past.Node.(*proto.Ast_Lookup))
	case *proto.Ast_Ifelse:
		return fromProtoIfelse(past.Node.(*proto.Ast_Ifelse))
	default:
		return null, fmt.Errorf("invalid proto ast: %v", past)
	}
}

//=====================================
// Satisfy ast interface requirements
//=====================================

func (l Lookup) toProto() (proto.Ast, error) {
	pon, err := ToProtoAst(l.On)
	if err != nil {
		return pnull, err
	}
	return proto.Ast{Node: &proto.Ast_Lookup{Lookup: &proto.Lookup{
		On:       &pon,
		Property: l.Property,
	}}}, nil
}

func (a At) toProto() (proto.Ast, error) {
	return proto.Ast{Node: &proto.Ast_At{At: &proto.At{}}}, nil
}

func (atom Atom) toProto() (proto.Ast, error) {
	switch atom.Type {
	case Int:
		n, err := strconv.ParseInt(atom.Lexeme, 10, 64)
		if err == nil {
			return proto.Ast{Node: &proto.Ast_Atom{Atom: &proto.Atom{Inner: &proto.Atom_Int{Int: n}}}}, nil
		} else {
			return pnull, err
		}
	case Double:
		d, err := strconv.ParseFloat(atom.Lexeme, 64)
		if err == nil {
			return proto.Ast{Node: &proto.Ast_Atom{Atom: &proto.Atom{Inner: &proto.Atom_Double{Double: d}}}}, nil
		} else {
			return pnull, err
		}

	case Bool:
		b, err := strconv.ParseBool(atom.Lexeme)
		if err == nil {
			return proto.Ast{Node: &proto.Ast_Atom{Atom: &proto.Atom{Inner: &proto.Atom_Bool{Bool: b}}}}, nil
		} else {
			return pnull, err
		}
	case String:
		return proto.Ast{Node: &proto.Ast_Atom{Atom: &proto.Atom{Inner: &proto.Atom_String_{String_: atom.Lexeme}}}}, nil
	default:
		return pnull, fmt.Errorf("invalid atom type: %v", atom.Type)
	}

}

func (binary Binary) toProto() (proto.Ast, error) {
	protoLeft, err := ToProtoAst(binary.Left)
	if err != nil {
		return pnull, err
	}
	protoRight, err := ToProtoAst(binary.Right)
	if err != nil {
		return pnull, err
	}
	return proto.Ast{Node: &proto.Ast_Binary{Binary: &proto.Binary{
		Left:  &protoLeft,
		Right: &protoRight,
		Op:    binary.Op,
	}}}, nil
}

func (list List) toProto() (proto.Ast, error) {
	ret := make([]*proto.Ast, len(list.Values))
	for i, ast := range list.Values {
		past, err := ToProtoAst(ast)
		if err != nil {
			return pnull, err
		}
		ret[i] = &past
	}
	return proto.Ast{Node: &proto.Ast_List{List: &proto.List{Values: ret}}}, nil
}

func (s Statement) toProto() (proto.Ast, error) {
	pbody, err := ToProtoAst(s.Body)
	if err != nil {
		return pnull, err
	}
	return proto.Ast{Node: &proto.Ast_Statement{Statement: &proto.Statement{
		Name: s.Name,
		Body: &pbody,
	}}}, nil
}

func (q Query) toProto() (proto.Ast, error) {
	ret := make([]*proto.Statement, len(q.Statements))
	for i, s := range q.Statements {
		ps, err := ToProtoAst(s)
		if err != nil {
			return pnull, err
		}
		ret[i] = ps.GetStatement()
	}
	return proto.Ast{Node: &proto.Ast_Query{Query: &proto.Query{Statements: ret}}}, nil
}

func (d Dict) toProto() (proto.Ast, error) {
	ret := make(map[string]*proto.Ast, len(d.Values))
	for k, ast := range d.Values {
		past, err := ToProtoAst(ast)
		if err != nil {
			return pnull, err
		}
		ret[k] = &past
	}
	return proto.Ast{Node: &proto.Ast_Dict{Dict: &proto.Dict{Values: ret}}}, nil
}

func (opcall OpCall) toProto() (proto.Ast, error) {
	poperand, err := ToProtoAst(opcall.Operand)
	if err != nil {
		return pnull, err
	}

	pdict, err := ToProtoAst(opcall.Kwargs)
	if err != nil {
		return pnull, err
	}
	return proto.Ast{Node: &proto.Ast_Opcall{Opcall: &proto.OpCall{
		Operand:   &poperand,
		Namespace: opcall.Namespace,
		Name:      opcall.Name,
		Kwargs:    pdict.GetDict(),
	}}}, nil
}

func (v Var) toProto() (proto.Ast, error) {
	return proto.Ast{Node: &proto.Ast_Var{Var: &proto.Var{Name: v.Name}}}, nil
}

func (table Table) toProto() (proto.Ast, error) {
	pinner, err := ToProtoAst(table.Inner)
	if err != nil {
		return pnull, err
	}
	return proto.Ast{Node: &proto.Ast_Table{Table: &proto.Table{Inner: &pinner}}}, nil
}

func (ifelse IfElse) toProto() (proto.Ast, error) {
	protoCondition, err := ToProtoAst(ifelse.Condition)
	if err != nil {
		return pnull, err
	}
	protoThenDo, err := ToProtoAst(ifelse.ThenDo)
	if err != nil {
		return pnull, err
	}
	protoElseDo, err := ToProtoAst(ifelse.ElseDo)
	if err != nil {
		return pnull, err
	}
	return proto.Ast{Node: &proto.Ast_Ifelse{Ifelse: &proto.IfElse{
		Condition: &protoCondition,
		ThenDo:    &protoThenDo,
		ElseDo:    &protoElseDo,
	}}}, nil
}

//=============================
// More private helpers below
//=============================

var pnull = proto.Ast{}
var null = Atom{}

func fromProtoAt(pat *proto.Ast_At) (Ast, error) {
	return At{}, nil
}

func fromProtoLookup(plookup *proto.Ast_Lookup) (Ast, error) {
	on, err := FromProtoAst(*plookup.Lookup.On)
	if err != nil {
		return null, err
	}
	return Lookup{On: on, Property: plookup.Lookup.Property}, nil
}

func fromProtoTable(ptable *proto.Ast_Table) (Ast, error) {
	table, err := FromProtoAst(*ptable.Table.Inner)
	if err != nil {
		return null, err
	}
	return Table{Inner: table}, nil
}

func fromProtoVar(pvar *proto.Ast_Var) (Ast, error) {
	return Var{Name: pvar.Var.Name}, nil
}

func fromProtoOpcall(popcall *proto.Ast_Opcall) (Ast, error) {
	operand, err := FromProtoAst(*popcall.Opcall.Operand)
	if err != nil {
		return null, err
	}
	dict, err := FromProtoAst(proto.Ast{Node: &proto.Ast_Dict{Dict: popcall.Opcall.Kwargs}})
	if err != nil {
		return null, err
	}
	return OpCall{
		Operand:   operand,
		Namespace: popcall.Opcall.Namespace,
		Name:      popcall.Opcall.Name,
		Kwargs:    dict.(Dict),
	}, nil
}

func fromProtoQuery(pquery *proto.Ast_Query) (Ast, error) {
	statements := make([]Statement, len(pquery.Query.Statements))
	for i, ps := range pquery.Query.Statements {
		s, err := FromProtoAst(proto.Ast{Node: &proto.Ast_Statement{Statement: ps}})
		if err != nil {
			return null, err
		}
		statements[i] = s.(Statement)
	}
	return Query{Statements: statements}, nil
}

func fromProtoStatement(pstatement *proto.Ast_Statement) (Ast, error) {
	body, err := FromProtoAst(*pstatement.Statement.Body)
	if err != nil {
		return null, err
	}
	return Statement{
		Name: pstatement.Statement.Name,
		Body: body,
	}, nil
}

func fromProtoAtom(patom *proto.Ast_Atom) (Ast, error) {
	if pint, ok := patom.Atom.Inner.(*proto.Atom_Int); ok {
		return Atom{Type: Int, Lexeme: fmt.Sprintf("%d", pint.Int)}, nil
	}
	if pdouble, ok := patom.Atom.Inner.(*proto.Atom_Double); ok {
		// when printing back to float, don't add trailing zeros
		return Atom{Type: Double, Lexeme: strconv.FormatFloat(pdouble.Double, 'f', -1, 64)}, nil
	}
	if pbool, ok := patom.Atom.Inner.(*proto.Atom_Bool); ok {
		var str string
		if pbool.Bool {
			str = "true"
		} else {
			str = "false"
		}
		return Atom{Type: Bool, Lexeme: str}, nil
	}
	if pstr, ok := patom.Atom.Inner.(*proto.Atom_String_); ok {
		return Atom{Type: String, Lexeme: pstr.String_}, nil
	}
	return null, fmt.Errorf("invalid proto atom: %v", patom)
}

func fromProtoList(plist *proto.Ast_List) (Ast, error) {
	pvalues := plist.List.Values
	values := make([]Ast, len(pvalues))
	for i, pv := range pvalues {
		v, err := FromProtoAst(*pv)
		if err != nil {
			return null, err
		}
		values[i] = v
	}
	return List{Values: values}, nil
}

func fromProtoDict(plist *proto.Ast_Dict) (Ast, error) {
	pvalues := plist.Dict.Values
	values := make(map[string]Ast, len(pvalues))
	for i, pv := range pvalues {
		v, err := FromProtoAst(*pv)
		if err != nil {
			return null, err
		}
		values[i] = v
	}
	return Dict{Values: values}, nil
}

func fromProtoBinary(pbin *proto.Ast_Binary) (Ast, error) {
	left, err := FromProtoAst(*pbin.Binary.Left)
	if err != nil {
		return null, err
	}
	right, err := FromProtoAst(*pbin.Binary.Right)
	if err != nil {
		return null, err
	}
	return Binary{
		Left:  left,
		Op:    pbin.Binary.Op,
		Right: right,
	}, nil
}

func fromProtoIfelse(pifelse *proto.Ast_Ifelse) (Ast, error) {
	condition, err := FromProtoAst(*pifelse.Ifelse.Condition)
	if err != nil {
		return null, err
	}
	thenDo, err := FromProtoAst(*pifelse.Ifelse.ThenDo)
	if err != nil {
		return null, err
	}
	elseDo, err := FromProtoAst(*pifelse.Ifelse.ElseDo)
	if err != nil {
		return null, err
	}
	return IfElse{
		Condition: condition,
		ThenDo:    thenDo,
		ElseDo:    elseDo,
	}, nil
}
