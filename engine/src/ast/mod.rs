use crate::lexer::Token;
use std::collections::HashMap;

pub mod eval;
pub mod printer;

// using this for sorted iterator
pub struct OpCall<'a> {
    pub path: Vec<Token<'a>>,
    pub args: HashMap<Token<'a>, Ast<'a>>,
}

pub enum Ast<'a> {
    Binary {
        left: Box<Ast<'a>>,
        op: Token<'a>,
        right: Box<Ast<'a>>,
    },
    Grouping(Box<Ast<'a>>),
    Unary(Token<'a>, Box<Ast<'a>>),
    Atom(Token<'a>),
    List(Vec<Ast<'a>>),
    Record(Vec<Token<'a>>, Vec<Ast<'a>>),
    OpExp(Box<Ast<'a>>, Vec<OpCall<'a>>),
    Statement(Option<Token<'a>>, Box<Ast<'a>>),
    Query(Vec<Ast<'a>>),
}

impl<'a> Ast<'a> {
    pub fn accept<T>(&self, visitor: &dyn Visitor<T>) -> T {
        match self {
            Ast::Binary { left, op, right } => visitor.visit_binary(left, op, right),
            Ast::Grouping(inner) => visitor.visit_grouping(inner),
            Ast::Unary(t, b) => visitor.visit_unary(t, b),
            Ast::Atom(t) => visitor.visit_atom(t),
            Ast::List(l) => visitor.visit_list(l),
            Ast::Record(names, values) => visitor.visit_record(names, values),
            Ast::OpExp(root, opcalls) => visitor.visit_opexp(root, opcalls),
            Ast::Statement(v, b) => visitor.visit_statement(v, b),
            Ast::Query(q) => visitor.visit_query(q),
        }
    }
}

pub trait Visitor<T> {
    fn visit_binary(&self, left: &Ast, op: &Token, right: &Ast) -> T;
    fn visit_grouping(&self, inner: &Ast) -> T;
    fn visit_unary(&self, op: &Token, right: &Ast) -> T;
    fn visit_list(&self, list: &[Ast]) -> T;
    fn visit_atom(&self, t: &Token) -> T;
    fn visit_record(&self, names: &[Token], values: &[Ast]) -> T;
    fn visit_opexp(&self, root: &Ast, opcalls: &[OpCall]) -> T;
    fn visit_statement(&self, variable: &Option<Token>, body: &Ast) -> T;
    fn visit_query(&self, statements: &[Ast]) -> T;
}
