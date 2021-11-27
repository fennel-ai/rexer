use crate::lexer::Token;
use std::collections::HashMap;

mod eval;
mod printer;

// using this for sorted iterator
pub struct OpCall {
    pub path: Vec<Token>,
    pub args: HashMap<Token, Ast>,
}

pub enum Ast {
    Binary {
        left: Box<Ast>,
        op: Token,
        right: Box<Ast>,
    },
    Grouping(Box<Ast>),
    Unary(Token, Box<Ast>),
    Atom(Token),
    List(Vec<Ast>),
    Record(HashMap<Token, Ast>),
    OpExp(Box<Ast>, Vec<OpCall>),
    Statement(Option<Token>, Box<Ast>),
    Query(Vec<Ast>),
}

impl Ast {
    pub fn accept<T>(&self, visitor: &dyn Visitor<T>) -> T {
        match self {
            Ast::Binary { left, op, right } => visitor.visit_binary(left, op, right),
            Ast::Grouping(inner) => visitor.visit_grouping(inner),
            Ast::Unary(t, b) => visitor.visit_unary(t, b),
            Ast::Atom(t) => visitor.visit_atom(t),
            Ast::List(l) => visitor.visit_list(l),
            Ast::Record(r) => visitor.visit_record(r),
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
    fn visit_record(&self, record: &HashMap<Token, Ast>) -> T;
    fn visit_opexp(&self, root: &Ast, opcalls: &[OpCall]) -> T;
    fn visit_statement(&self, variable: &Option<Token>, body: &Ast) -> T;
    fn visit_query(&self, statements: &[Ast]) -> T;
}
