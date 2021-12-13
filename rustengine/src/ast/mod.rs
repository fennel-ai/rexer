use crate::lexer::Token;
use std::collections::HashMap;

pub mod eval;
pub mod localfinder;
pub mod printer;

// using this for sorted iterator
pub struct OpCall<'q> {
    pub path: Vec<Token<'q>>,
    pub args: HashMap<Token<'q>, Ast<'q>>,
}

pub enum Ast<'q> {
    Binary {
        left: Box<Ast<'q>>,
        op: Token<'q>,
        right: Box<Ast<'q>>,
    },
    Grouping(Box<Ast<'q>>),
    Unary(Token<'q>, Box<Ast<'q>>),
    Atom(Token<'q>),
    List(Vec<Ast<'q>>),
    Record(Vec<Token<'q>>, Vec<Ast<'q>>),
    OpExp(Box<Ast<'q>>, Vec<OpCall<'q>>),
    Statement(Option<Token<'q>>, Box<Ast<'q>>),
    Query(Vec<Ast<'q>>),
}

impl<'q> Ast<'q> {
    pub fn accept<T>(&'q self, visitor: &mut dyn Visitor<'q, T>) -> T {
        match self {
            Ast::Binary { left, op, right } => visitor.visit_binary(left, op, right),
            Ast::Grouping(inner) => visitor.visit_grouping(inner),
            Ast::Unary(t, b) => visitor.visit_unary(t, b),
            Ast::Atom(t) => visitor.visit_atom(t),
            Ast::List(l) => visitor.visit_list(l),
            Ast::Record(names, values) => visitor.visit_record(names, values),
            Ast::OpExp(root, opcalls) => visitor.visit_opexp(root, opcalls),
            Ast::Statement(v, b) => visitor.visit_statement(v.as_ref(), b),
            Ast::Query(q) => visitor.visit_query(q),
        }
    }
}

pub trait Visitor<'q, T> {
    fn visit_binary(&mut self, left: &'q Ast<'q>, op: &'q Token<'q>, right: &'q Ast<'q>) -> T;
    fn visit_grouping(&mut self, inner: &'q Ast<'q>) -> T;
    fn visit_unary(&mut self, op: &'q Token<'q>, right: &'q Ast<'q>) -> T;
    fn visit_list(&mut self, list: &'q [Ast<'q>]) -> T;
    fn visit_atom(&mut self, t: &'q Token<'q>) -> T;
    fn visit_record(&mut self, names: &'q [Token<'q>], values: &'q [Ast<'q>]) -> T;
    fn visit_opexp(&mut self, root: &'q Ast<'q>, opcalls: &'q [OpCall<'q>]) -> T;
    fn visit_statement(&mut self, variable: Option<&'q Token<'q>>, body: &'q Ast<'q>) -> T;
    fn visit_query(&mut self, statements: &'q [Ast<'q>]) -> T;
}
