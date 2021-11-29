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

// 'a denotes the lifetime of slice (aka lifetime of parser)
// 'b denotes the lifetime of tokens
pub struct Parser<'a> {
    tokens: Vec<Token<'a>>,
    previous: Option<Token<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(mut tokens: Vec<Token<'a>>) -> Self {
        tokens.reverse();
        Parser {
            tokens: tokens,
            previous: None,
        }
    }

    fn peek(&self) -> Option<&Token<'a>> {
        self.tokens.last()
    }

    fn previous(&mut self) -> Option<Token<'a>> {
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

    fn consume(&mut self, token_type: TokenType) -> anyhow::Result<Token<'a>> {
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

    pub fn parse(&mut self) -> anyhow::Result<Ast<'a>> {
        self.query()
    }

    fn query(&mut self) -> anyhow::Result<Ast<'a>> {
        let mut statements = vec![self.statement()?];
        let semicolon = &[TokenType::Semicolon];
        while self.matches(semicolon) {
            if self.done() {
                break;
            }
            statements.push(self.statement()?);
        }
        // consume optional trailing semi colon
        self.matches(semicolon);
        Ok(Ast::Query(statements))
    }

    fn statement(&mut self) -> anyhow::Result<Ast<'a>> {
        let variable = if let Ok(s) = self.identifier() {
            self.consume(TokenType::Equal)?;
            Some(s)
        } else {
            None
        };
        let opexp = self.op_expression()?;
        Ok(Ast::Statement(variable, Box::new(opexp)))
    }

    fn op_expression(&mut self) -> anyhow::Result<Ast<'a>> {
        let e = self.expression()?;
        let mut opcalls: Vec<OpCall> = vec![];
        let pipe = &[TokenType::Pipe];
        while self.matches(pipe) {
            opcalls.push(self.opcall()?);
        }
        Ok(Ast::OpExp(Box::new(e), opcalls))
    }

    fn opcall(&mut self) -> anyhow::Result<OpCall<'a>> {
        let mut path: Vec<Token> = vec![];
        let dot = &[TokenType::Dot];
        loop {
            path.push(self.identifier()?);
            if !self.matches(dot) {
                break;
            }
        }
        let mut args = HashMap::new();
        self.consume(TokenType::LeftParen)?;
        let comma = &[TokenType::Comma];
        loop {
            let k = self.identifier()?;
            self.consume(TokenType::Equal)?;
            let e = self.expression()?;
            args.insert(k, e);
            if !self.matches(comma) {
                break;
            }
            if self.check(TokenType::RightParen) {
                break;
            }
        }
        self.consume(TokenType::RightParen)?;
        Ok(OpCall { path, args })
    }

    fn identifier(&mut self) -> anyhow::Result<Token<'a>> {
        self.consume(TokenType::Identifier)
    }

    fn expression(&mut self) -> anyhow::Result<Ast<'a>> {
        self.logic_or()
    }

    fn term(&mut self) -> anyhow::Result<Ast<'a>> {
        let mut f = self.factor()?;
        let expected = &[TokenType::Plus, TokenType::Minus];
        while self.matches(expected) {
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

    fn factor(&mut self) -> anyhow::Result<Ast<'a>> {
        let mut u = self.unary()?;
        let expected = &[TokenType::Star, TokenType::Slash];
        while self.matches(expected) {
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

    fn unary(&mut self) -> anyhow::Result<Ast<'a>> {
        let ops = &[TokenType::Minus];
        if self.matches(ops) {
            let op = self.previous().unwrap();
            let right = self.unary()?;
            Ok(Ast::Unary(op, Box::new(right)))
        } else {
            self.primary()
        }
    }

    fn list(&mut self) -> anyhow::Result<Ast<'a>> {
        let mut l = vec![];
        let comma = &[TokenType::Comma];
        while !self.check(TokenType::ListEnd) {
            let e = self.expression()?;
            l.push(e);
            if !self.matches(comma) {
                break;
            }
        }
        self.consume(TokenType::ListEnd)?;
        Ok(Ast::List(l))
    }

    fn record(&mut self) -> anyhow::Result<Ast<'a>> {
        let mut r = HashMap::new();
        let comma = &[TokenType::Comma];
        while !self.check(TokenType::RecordEnd) {
            let id = self.identifier()?;
            self.consume(TokenType::Equal)?;
            let e = self.expression()?;
            r.insert(id, e);
            if !self.matches(comma) {
                break;
            }
        }
        self.consume(TokenType::RecordEnd)?;
        Ok(Ast::Record(r))
    }

    fn primary(&mut self) -> anyhow::Result<Ast<'a>> {
        if self.matches(&vec![
            TokenType::Number,
            TokenType::String,
            TokenType::Bool,
            TokenType::Variable,
            TokenType::Identifier,
        ]) {
            Ok(Ast::Atom(self.previous().unwrap()))
        } else if self.matches(&[TokenType::LeftParen]) {
            let e = self.expression();
            self.consume(TokenType::RightParen)?;
            e
        } else if self.matches(&[TokenType::ListBegin]) {
            self.list()
        } else if self.matches(&[TokenType::RecordBegin]) {
            self.record()
        } else {
            anyhow::bail!("Unexpected token: {:?}", self.peek())
        }
    }

    fn comparison(&mut self) -> anyhow::Result<Ast<'a>> {
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
    fn equality(&mut self) -> anyhow::Result<Ast<'a>> {
        let mut f = self.comparison()?;
        let expected = &[TokenType::EqualEqual, TokenType::BangEqual];
        while self.matches(expected) {
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
    fn logic_and(&mut self) -> anyhow::Result<Ast<'a>> {
        let mut f = self.equality()?;
        while self.matches(&[TokenType::And]) {
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
    fn logic_or(&mut self) -> anyhow::Result<Ast<'a>> {
        let mut f = self.logic_and()?;
        while self.matches(&[TokenType::Or]) {
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
