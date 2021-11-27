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
/// primary := NUMBER | STRING | "true" | "false" | "(" expression ")" | list | record
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
use crate::lexer::{Token, TokenType, TokenValue};
use std::collections::HashMap;

// using this for sorted iterator
use itertools::Itertools;
use std::fmt;
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

    fn matches(&mut self, token_types: &Vec<TokenType>) -> bool {
        for t in token_types {
            if self.check(t) {
                self.advance();
                return true;
            }
        }
        return false;
    }

    fn check(&self, token_type: &TokenType) -> bool {
        if self.done() {
            return false;
        }
        if let Some(token) = self.peek() {
            if token.token_type == *token_type {
                return true;
            };
        }
        return false;
    }

    fn consume(&mut self, token_type: &TokenType) -> anyhow::Result<Token> {
        if self.check(token_type) {
            self.advance();
            return self.previous().ok_or(anyhow::anyhow!("missing"));
        } else {
            return Err(anyhow::anyhow!(
                "Unexpected token: {:?}. Expected token of type: {:?}",
                self.peek(),
                token_type,
            ));
        }
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
            self.consume(&TokenType::Equal)?;
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
        let mut path: Vec<String> = vec![];

        loop {
            path.push(self.identifier()?);
            if !self.matches(&vec![TokenType::Dot]) {
                break;
            }
        }
        let mut args = HashMap::new();
        self.consume(&TokenType::LeftParen)?;
        loop {
            let k = self.identifier()?;
            self.consume(&TokenType::Equal)?;
            let e = self.expression()?;
            args.insert(k, e);
            if !self.matches(&vec![TokenType::Comma]) {
                break;
            }
            if self.check(&TokenType::RightParen) {
                break;
            }
        }
        self.consume(&TokenType::RightParen)?;
        Ok(OpCall { path, args })
    }

    fn identifier(&mut self) -> anyhow::Result<String> {
        match self.consume(&TokenType::Identifier)?.literal {
            Some(TokenValue::String(s)) => Ok(s),
            _ => anyhow::bail!("Expected string as key, found: "),
        }
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
            f = Ast::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }

    fn factor(&mut self) -> anyhow::Result<Ast> {
        let mut u = self.unary()?;
        let expected = vec![TokenType::Star, TokenType::Slash];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.unary()?;
            u = Ast::Binary(BinaryExpr {
                op: op,
                left: Box::new(u),
                right: Box::new(right),
            })
        }
        Ok(u)
    }

    fn unary(&mut self) -> anyhow::Result<Ast> {
        if self.matches(&vec![TokenType::Minus]) {
            let op = self.previous().unwrap();
            let right = self.unary()?;
            return Ok(Ast::Unary(UnaryExpr {
                op: op,
                right: Box::new(right),
            }));
        }
        self.primary()
    }

    fn list(&mut self) -> anyhow::Result<Ast> {
        let mut l = vec![];
        while !self.check(&TokenType::ListEnd) {
            let e = self.expression()?;
            l.push(e);
            if !self.matches(&vec![TokenType::Comma]) {
                break;
            }
        }
        self.consume(&TokenType::ListEnd)?;
        Ok(Ast::List(l))
    }

    fn record(&mut self) -> anyhow::Result<Ast> {
        let mut r = HashMap::new();
        while !self.check(&TokenType::RecordEnd) {
            let id = self.identifier()?;
            self.consume(&TokenType::Equal)?;
            let e = self.expression()?;
            r.insert(id, e);
            if !self.matches(&vec![TokenType::Comma]) {
                break;
            }
        }
        self.consume(&TokenType::RecordEnd)?;
        Ok(Ast::Record(r))
    }

    fn primary(&mut self) -> anyhow::Result<Ast> {
        if self.matches(&vec![TokenType::LeftParen]) {
            let e = self.expression();
            self.consume(&TokenType::RightParen)?;
            e
        } else if self.matches(&vec![
            TokenType::Number,
            TokenType::String,
            TokenType::True,
            TokenType::False,
        ]) {
            Ok(Ast::Literal(self.previous().unwrap()))
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
            f = Ast::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }
    fn equality(&mut self) -> anyhow::Result<Ast> {
        let mut f = self.comparison()?;
        let expected = vec![TokenType::EqualEqual, TokenType::BangEqual];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.comparison()?;
            f = Ast::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }
    fn logic_and(&mut self) -> anyhow::Result<Ast> {
        let mut f = self.equality()?;
        while self.matches(&vec![TokenType::And]) {
            let op = self.previous().unwrap();
            let right = self.equality()?;
            f = Ast::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }
    fn logic_or(&mut self) -> anyhow::Result<Ast> {
        let mut f = self.logic_and()?;
        while self.matches(&vec![TokenType::Or]) {
            let op = self.previous().unwrap();
            let right = self.logic_and()?;
            f = Ast::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }
}

struct BinaryExpr {
    left: Box<Ast>,
    op: Token,
    right: Box<Ast>,
}

struct UnaryExpr {
    op: Token,
    right: Box<Ast>,
}

struct OpCall {
    path: Vec<String>,
    args: HashMap<String, Ast>,
}

enum Ast {
    Binary(BinaryExpr),
    Grouping(Box<Ast>),
    Unary(UnaryExpr),
    Literal(Token),
    List(Vec<Ast>),
    Record(HashMap<String, Ast>),
    OpExp(Box<Ast>, Vec<OpCall>),
    Statement(Option<String>, Box<Ast>),
    Query(Vec<Ast>),
}

impl Ast {
    pub fn accept<T>(&self, visitor: &dyn Visitor<T>) -> T {
        match self {
            Ast::Binary(inner) => visitor.visit_binary(inner),
            Ast::Grouping(inner) => visitor.visit_grouping(inner),
            Ast::Unary(inner) => visitor.visit_unary(inner),
            Ast::Literal(t) => visitor.visit_literal(t),
            Ast::List(l) => visitor.visit_list(l),
            Ast::Record(r) => visitor.visit_record(r),
            Ast::OpExp(root, opcalls) => visitor.visit_opexp(root, opcalls),
            Ast::Statement(v, b) => visitor.visit_statement(v, b),
            Ast::Query(q) => visitor.visit_query(q),
        }
    }
}

impl fmt::Display for Ast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.accept(&AstPrinter {}))
    }
}

trait Visitor<T> {
    fn visit_binary(&self, binary: &BinaryExpr) -> T;
    fn visit_grouping(&self, inner: &Ast) -> T;
    fn visit_unary(&self, unary: &UnaryExpr) -> T;
    fn visit_literal(&self, literal: &Token) -> T;
    fn visit_list(&self, list: &[Ast]) -> T;
    fn visit_record(&self, record: &HashMap<String, Ast>) -> T;
    fn visit_opexp(&self, root: &Box<Ast>, opcalls: &Vec<OpCall>) -> T;
    fn visit_statement(&self, variable: &Option<String>, body: &Ast) -> T;
    fn visit_query(&self, statements: &Vec<Ast>) -> T;
}

struct AstPrinter {}

impl Visitor<String> for AstPrinter {
    fn visit_binary(&self, binary: &BinaryExpr) -> String {
        // lisp-style formatting.
        return format!(
            "({} {} {})",
            binary.op.lexeme,
            binary.left.accept(self),
            binary.right.accept(self)
        );
    }
    fn visit_grouping(&self, inner: &Ast) -> String {
        return format!("(group {})", inner.accept(self));
    }
    fn visit_unary(&self, unary: &UnaryExpr) -> String {
        return format!("({} {})", unary.op.lexeme, unary.right.accept(self));
    }

    fn visit_literal(&self, token: &Token) -> String {
        return token.lexeme.clone();
    }

    fn visit_list(&self, list: &[Ast]) -> String {
        return format!("[{}]", list.iter().map(|e| e.accept(self)).join(", "));
    }

    fn visit_record(&self, record: &HashMap<String, Ast>) -> String {
        format!(
            "{{{}}}",
            record
                .iter()
                .map(|(k, v)| format!("{}={}", k, v.accept(self)))
                .sorted()
                .join(", ")
        )
    }
    fn visit_opexp(&self, root: &Box<Ast>, opcalls: &Vec<OpCall>) -> String {
        if opcalls.len() == 0 {
            root.accept(self)
        } else {
            let opcallstr = opcalls
                .iter()
                .map(|opcall| {
                    format!(
                        "{}({})",
                        opcall.path.join("."),
                        opcall
                            .args
                            .iter()
                            .map(|(k, v)| format!("{}={}", k, v.accept(self)))
                            .sorted()
                            .join(", ")
                    )
                })
                .join(" | ");
            format!("{} | {}", root.accept(self), opcallstr)
        }
    }
    fn visit_statement(&self, variable: &Option<String>, body: &Ast) -> String {
        let assignment = if let Some(s) = variable {
            format!("{} = ", s)
        } else {
            "".to_string()
        };
        format!("{}{}", assignment, body.accept(self))
    }
    fn visit_query(&self, query: &Vec<Ast>) -> String {
        query.iter().map(|s| s.accept(self)).join(";\n")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::lexer::{Token, TokenType, TokenValue};

    use super::Ast;
    use super::AstPrinter;
    use super::BinaryExpr;
    use super::UnaryExpr;

    fn _compare_printed(exprstr: String, expected: String) {
        let lexer = Lexer::new(exprstr);
        let mut start = Instant::now();
        let tokens = lexer.tokenize().unwrap();
        let mut time = start.elapsed();
        println!("Time to lex: {:?}", time);
        let mut parser = Parser::new(tokens);
        start = Instant::now();
        let expr = parser.query().unwrap();
        time = start.elapsed();
        println!("Time to parse: {:?}", time);
        let printer = AstPrinter {};
        let actual = expr.accept(&printer);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_ast_pretty_print() {
        let printer = AstPrinter {};
        let expr = Ast::Binary(BinaryExpr {
            left: Box::new(Ast::Unary(UnaryExpr {
                op: Token {
                    token_type: TokenType::Plus,
                    lexeme: "+".to_string(),
                    literal: None,
                },
                right: Box::new(Ast::Literal(Token {
                    token_type: TokenType::Number,
                    literal: Some(TokenValue::Double(123 as f64)),
                    lexeme: "123".to_string(),
                })),
            })),
            op: Token {
                token_type: TokenType::Pipe,
                lexeme: "|".to_string(),
                literal: None,
            },
            right: Box::new(Ast::Grouping(Box::new(Ast::Literal(Token {
                token_type: TokenType::Number,
                lexeme: "45.67".to_string(),
                literal: Some(TokenValue::Double(45.67)),
            })))),
        });
        let actual = expr.accept(&printer);
        let expected = "(| (+ 123) (group 45.67))";
        assert_eq!(actual, expected)
    }

    use super::Parser;
    use crate::lexer::Lexer;

    #[test]
    fn end_to_end() {
        let exprstr = "1 + 2 * 3".to_string();
        let expected = "(+ 1 (* 2 3))".to_string();
        _compare_printed(exprstr, expected);
    }

    #[test]
    fn with_booleans() {
        let exprstr = "2 >= 3 or true and 1 == 2".to_string();
        let expected = "(or (>= 2 3) (and true (== 1 2)))".to_string();
        _compare_printed(exprstr, expected);
    }

    #[test]
    fn parse_list() {
        let exprstr = "[1,2,3]".to_string();
        let expected = "[1, 2, 3]".to_string();
        _compare_printed(exprstr, expected)
    }

    #[test]
    fn parse_record() {
        let first = "{xyz=123, foo=\"bar\"}".to_string();
        let second = "{foo=\"bar\",xyz=123}".to_string();
        let expected = "{foo=\"bar\", xyz=123}".to_string();
        _compare_printed(first, expected.clone());
        _compare_printed(second, expected);
    }

    #[test]
    fn parse_opexp() {
        let expstr = "12 | a.b.c(x=123, y=\"hi\",)".to_string();
        let expected = "12 | a.b.c(x=123, y=\"hi\")".to_string();
        _compare_printed(expstr, expected);
    }

    #[test]
    fn parse_statement() {
        let expstr = "name = 12 | a.b.c(x=123, y=\"hi\",)".to_string();
        let expected = "name = 12 | a.b.c(x=123, y=\"hi\")".to_string();
        _compare_printed(expstr, expected);
        _compare_printed("5".to_string(), "5".to_string());
    }
    #[test]
    fn parse_program() {
        let expstr = "name = 12 | a.b.c(x=123, y=\"hi\",); abc = 8; 5;".to_string();
        let expected = "name = 12 | a.b.c(x=123, y=\"hi\");\nabc = 8;\n5".to_string();
        _compare_printed(expstr, expected);
    }
}
