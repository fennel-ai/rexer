use crate::ast::{Ast, OpCall};
use crate::lexer::{Token, TokenType};
use std::collections::HashMap;

/// StarQL Grammar:
///
/// query :=  statement (";" statement) * ";"?
/// statement := (identifier "=")? op_expression
/// op_expression := expression | ( "|" opcall) *
/// opcall := identifier ("." identifier) * "(" parameters ")"
/// expression := logic_or
/// logic_or := logic_and ( "or" logic_and )*
/// logic_and := equality ( "and" equality )*
/// equality := comparison ( ( "!=" | "==" ) comparison )*
/// comparison := term ( ( ">" | ">=" | "<" | "<=" ) term )*
/// term := factor (("+" | "-") factor)*
/// factor := unary (("*" | "/") unary)*
/// unary := primary | "-" unary
/// symbol := "$" identifier
/// primary := NUMBER | STRING | "true" | "false" | "(" expression ")" | list | record | symbol
/// list := "[" (expression ",")* expression? "]"
/// record := "{" parameters "}"
/// parameters := (identifier "=" expression ",")* (identifier "=" expression)?
///
/// To add:
/// * Booleans
/// * Symbols
/// Plan:
///     add bool, string, number in expr enum
/// TODOs:
///     replace anyhow::Result with our own ParseError error class?
///

pub struct Parser {
    tokens: Vec<Token>,
    previous: Option<Token>,
}

impl Parser {
    pub fn new(mut tokens: Vec<Token>) -> Self {
        tokens.reverse();
        Parser {
            tokens: tokens,
            previous: None,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.last()
    }

    fn previous(&mut self) -> Option<Token> {
        self.previous.take()
    }

    fn done(&self) -> bool {
        if let Some(t) = self.peek() {
            if t.token_type == TokenType::Eof {
                return true;
            }
        }
        return false;
    }

    fn advance(&mut self) {
        if !self.done() {
            self.previous = self.tokens.pop();
        }
    }

    fn matches(&mut self, token_types: &[TokenType]) -> bool {
        for t in token_types {
            if self.check(*t) {
                self.advance();
                return true;
            }
        }
        return false;
    }

    fn check(&self, token_type: TokenType) -> bool {
        if self.done() {
            return false;
        }
        if let Some(token) = self.peek() {
            if token.token_type == token_type {
                return true;
            };
        }
        return false;
    }

    fn consume(&mut self, token_type: TokenType) -> anyhow::Result<Token> {
        if self.check(token_type) {
            self.advance();
            return self.previous().ok_or_else(|| anyhow::anyhow!("missing"));
        } else {
            return Err(anyhow::anyhow!(
                "Unexpected token: {:?}. Expected token of type: {:?}",
                self.peek(),
                token_type,
            ));
        }
    }

    pub fn parse(&mut self) -> anyhow::Result<Ast> {
        self.query()
    }
    fn query(&mut self) -> anyhow::Result<Ast> {
        let mut statements = vec![self.statement()?];
        while self.matches(&vec![TokenType::Semicolon]) {
            if self.done() {
                break;
            }
            statements.push(self.statement()?);
        }
        // consume optional trailing semi colon
        self.matches(&vec![TokenType::Semicolon]);
        Ok(Ast::Query(statements))
    }

    fn statement(&mut self) -> anyhow::Result<Ast> {
        let variable = if let Ok(s) = self.identifier() {
            self.consume(TokenType::Equal)?;
            Some(s)
        } else {
            None
        };
        let opexp = self.op_expression()?;
        Ok(Ast::Statement(variable, Box::new(opexp)))
    }

    fn op_expression(&mut self) -> anyhow::Result<Ast> {
        let e = self.expression()?;
        let mut opcalls: Vec<OpCall> = vec![];
        while self.matches(&vec![TokenType::Pipe]) {
            opcalls.push(self.opcall()?);
        }
        Ok(Ast::OpExp(Box::new(e), opcalls))
    }

    fn opcall(&mut self) -> anyhow::Result<OpCall> {
        let mut path: Vec<Token> = vec![];
        loop {
            path.push(self.identifier()?);
            if !self.matches(&vec![TokenType::Dot]) {
                break;
            }
        }
        let mut args = HashMap::new();
        self.consume(TokenType::LeftParen)?;
        loop {
            let k = self.identifier()?;
            self.consume(TokenType::Equal)?;
            let e = self.expression()?;
            args.insert(k, e);
            if !self.matches(&vec![TokenType::Comma]) {
                break;
            }
            if self.check(TokenType::RightParen) {
                break;
            }
        }
        self.consume(TokenType::RightParen)?;
        Ok(OpCall { path, args })
    }

    fn identifier(&mut self) -> anyhow::Result<Token> {
        self.consume(TokenType::Identifier)
    }

    fn expression(&mut self) -> anyhow::Result<Ast> {
        self.logic_or()
    }

    fn term(&mut self) -> anyhow::Result<Ast> {
        let mut f = self.factor()?;
        let expected = vec![TokenType::Plus, TokenType::Minus];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.factor()?;
            f = Ast::Binary {
                left: Box::new(f),
                op: op,
                right: Box::new(right),
            }
        }
        Ok(f)
    }

    fn factor(&mut self) -> anyhow::Result<Ast> {
        let mut u = self.unary()?;
        let expected = vec![TokenType::Star, TokenType::Slash];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.unary()?;
            u = Ast::Binary {
                left: Box::new(u),
                op: op,
                right: Box::new(right),
            }
        }
        Ok(u)
    }

    fn unary(&mut self) -> anyhow::Result<Ast> {
        if self.matches(&vec![TokenType::Minus]) {
            let op = self.previous().unwrap();
            let right = self.unary()?;
            Ok(Ast::Unary(op, Box::new(right)))
        } else {
            self.primary()
        }
    }

    fn list(&mut self) -> anyhow::Result<Ast> {
        let mut l = vec![];
        while !self.check(TokenType::ListEnd) {
            let e = self.expression()?;
            l.push(e);
            if !self.matches(&vec![TokenType::Comma]) {
                break;
            }
        }
        self.consume(TokenType::ListEnd)?;
        Ok(Ast::List(l))
    }

    fn record(&mut self) -> anyhow::Result<Ast> {
        let mut r = HashMap::new();
        while !self.check(TokenType::RecordEnd) {
            let id = self.identifier()?;
            self.consume(TokenType::Equal)?;
            let e = self.expression()?;
            r.insert(id, e);
            if !self.matches(&vec![TokenType::Comma]) {
                break;
            }
        }
        self.consume(TokenType::RecordEnd)?;
        Ok(Ast::Record(r))
    }

    fn primary(&mut self) -> anyhow::Result<Ast> {
        if self.matches(&vec![
            TokenType::Number,
            TokenType::String,
            TokenType::Bool,
            TokenType::Variable,
            TokenType::Identifier,
        ]) {
            Ok(Ast::Atom(self.previous().unwrap()))
        } else if self.matches(&vec![TokenType::LeftParen]) {
            let e = self.expression();
            self.consume(TokenType::RightParen)?;
            e
        } else if self.matches(&vec![TokenType::ListBegin]) {
            self.list()
        } else if self.matches(&vec![TokenType::RecordBegin]) {
            self.record()
        } else {
            anyhow::bail!("Unexpcted token: {:?}", self.peek())
        }
    }

    fn comparison(&mut self) -> anyhow::Result<Ast> {
        let mut f = self.term()?;
        let expected = vec![
            TokenType::Greater,
            TokenType::GreaterEqual,
            TokenType::Lesser,
            TokenType::LesserEqual,
        ];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.term()?;
            f = Ast::Binary {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            }
        }
        Ok(f)
    }
    fn equality(&mut self) -> anyhow::Result<Ast> {
        let mut f = self.comparison()?;
        let expected = vec![TokenType::EqualEqual, TokenType::BangEqual];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.comparison()?;
            f = Ast::Binary {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            }
        }
        Ok(f)
    }
    fn logic_and(&mut self) -> anyhow::Result<Ast> {
        let mut f = self.equality()?;
        while self.matches(&vec![TokenType::And]) {
            let op = self.previous().unwrap();
            let right = self.equality()?;
            f = Ast::Binary {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            }
        }
        Ok(f)
    }
    fn logic_or(&mut self) -> anyhow::Result<Ast> {
        let mut f = self.logic_and()?;
        while self.matches(&vec![TokenType::Or]) {
            let op = self.previous().unwrap();
            let right = self.logic_and()?;
            f = Ast::Binary {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            }
        }
        Ok(f)
    }
}
