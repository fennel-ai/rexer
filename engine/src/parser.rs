/// StarQL Grammar:
///
/// expression := logic_or
/// logic_or := logic_and ( "or" logic_and )*
/// logic_and := equality ( "and" equality )*
/// equality := comparison ( ( "!=" | "==" ) comparison )*
/// comparison := term ( ( ">" | ">=" | "<" | "<=" ) term )*
/// term := factor (("+" | "-") factor)*
/// factor := unary (("*" | "/") unary)*
/// unary := primary | "-" unary
/// primary := NUMBER | STRING | "true" | "false" | "(" expression ")" | list
/// list := "[" (expression ",")* expression? "]"
///
/// To add:
/// * Records
/// * Lists
/// * Booleans
/// * assignment and variables
/// * operator calls
/// Plan:
///     add bool, string, number in expr enum
/// TODOs:
///     replace anyhow::Result with our own ParseError error class?
///
use crate::lexer::{Token, TokenType, TokenValue};
use std::collections::HashMap;

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
                "unexpected token: {:?}. Expected token of type: {:?}",
                self.peek(),
                token_type,
            ));
        }
    }

    fn expression(&mut self) -> anyhow::Result<Expr> {
        self.logic_or()
    }

    fn term(&mut self) -> anyhow::Result<Expr> {
        let mut f = self.factor()?;
        let expected = vec![TokenType::Plus, TokenType::Minus];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.factor()?;
            f = Expr::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }

    fn factor(&mut self) -> anyhow::Result<Expr> {
        let mut u = self.unary()?;
        let expected = vec![TokenType::Star, TokenType::Slash];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.unary()?;
            u = Expr::Binary(BinaryExpr {
                op: op,
                left: Box::new(u),
                right: Box::new(right),
            })
        }
        Ok(u)
    }

    fn unary(&mut self) -> anyhow::Result<Expr> {
        if self.matches(&vec![TokenType::Minus]) {
            let op = self.previous().unwrap();
            let right = self.unary()?;
            return Ok(Expr::Unary(UnaryExpr {
                op: op,
                right: Box::new(right),
            }));
        }
        self.primary()
    }

    fn list(&mut self) -> anyhow::Result<Expr> {
        let mut l = vec![];
        while !self.check(&TokenType::ListEnd) {
            let e = self.expression()?;
            l.push(e);
            if !self.matches(&vec![TokenType::Comma]) {
                break;
            }
        }
        self.consume(&TokenType::ListEnd)?;
        Ok(Expr::List(l))
    }

    fn record(&mut self) -> anyhow::Result<Expr> {
        let mut r = HashMap::new();
        while !self.check(&TokenType::RecordEnd) {
            let k = self.consume(&TokenType::Identifier)?;
            self.consume(&TokenType::Equal)?;
            let e = self.expression()?;
            if let TokenValue::String(id) = k.literal.unwrap() {
                r.insert(id, e);
            } else {
                // TODO: improve error.
                anyhow::bail!("Expected string as key, found: ");
            }
            if !self.matches(&vec![TokenType::Comma]) {
                break;
            }
        }
        self.consume(&TokenType::RecordEnd)?;
        Ok(Expr::Record(r))
    }

    fn primary(&mut self) -> anyhow::Result<Expr> {
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
            Ok(Expr::Literal(self.previous().unwrap()))
        } else if self.matches(&vec![TokenType::ListBegin]) {
            self.list()
        } else if self.matches(&vec![TokenType::RecordBegin]) {
            self.record()
        } else {
            anyhow::bail!("Unexpcted token: {:?}", self.peek())
        }
    }

    fn comparison(&mut self) -> anyhow::Result<Expr> {
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
            f = Expr::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }
    fn equality(&mut self) -> anyhow::Result<Expr> {
        let mut f = self.comparison()?;
        let expected = vec![TokenType::EqualEqual, TokenType::BangEqual];
        while self.matches(&expected) {
            let op = self.previous().unwrap();
            let right = self.comparison()?;
            f = Expr::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }
    fn logic_and(&mut self) -> anyhow::Result<Expr> {
        let mut f = self.equality()?;
        while self.matches(&vec![TokenType::And]) {
            let op = self.previous().unwrap();
            let right = self.equality()?;
            f = Expr::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }
    fn logic_or(&mut self) -> anyhow::Result<Expr> {
        let mut f = self.logic_and()?;
        while self.matches(&vec![TokenType::Or]) {
            let op = self.previous().unwrap();
            let right = self.logic_and()?;
            f = Expr::Binary(BinaryExpr {
                op: op,
                left: Box::new(f),
                right: Box::new(right),
            })
        }
        Ok(f)
    }
}

struct BinaryExpr {
    left: Box<Expr>,
    op: Token,
    right: Box<Expr>,
}

struct UnaryExpr {
    op: Token,
    right: Box<Expr>,
}

enum Expr {
    Binary(BinaryExpr),
    Grouping(Box<Expr>),
    Unary(UnaryExpr),
    Literal(Token),
    List(Vec<Expr>),
    Record(HashMap<String, Expr>),
}

impl Expr {
    pub fn accept<T>(&self, visitor: &dyn Visitor<T>) -> T {
        match self {
            Expr::Binary(inner) => {
                return visitor.visit_binary(inner);
            }
            Expr::Grouping(inner) => {
                return visitor.visit_grouping(inner);
            }
            Expr::Unary(inner) => {
                return visitor.visit_unary(inner);
            }
            Expr::Literal(t) => {
                return visitor.visit_literal(t);
            }
            Expr::List(l) => {
                return visitor.visit_list(l);
            }
            Expr::Record(r) => {
                return visitor.visit_record(r);
            }
        }
    }
}

trait Visitor<T> {
    fn visit_binary(&self, binary: &BinaryExpr) -> T;
    fn visit_grouping(&self, inner: &Expr) -> T;
    fn visit_unary(&self, unary: &UnaryExpr) -> T;
    fn visit_literal(&self, literal: &Token) -> T;
    fn visit_list(&self, list: &[Expr]) -> T;
    fn visit_record(&self, record: &HashMap<String, Expr>) -> T;
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
    fn visit_grouping(&self, inner: &Expr) -> String {
        return format!("(group {})", inner.accept(self));
    }
    fn visit_unary(&self, unary: &UnaryExpr) -> String {
        return format!("({} {})", unary.op.lexeme, unary.right.accept(self));
    }

    fn visit_literal(&self, token: &Token) -> String {
        return token.lexeme.clone();
    }

    fn visit_list(&self, list: &[Expr]) -> String {
        return format!(
            "[{}]",
            list.iter()
                .map(|e| e.accept(self))
                .collect::<Vec<String>>()
                .join(", ")
        );
    }

    fn visit_record(&self, record: &HashMap<String, Expr>) -> String {
        let mut pairs = record
            .iter()
            .map(|(k, v)| format!("{}={}", k, v.accept(self)))
            .collect::<Vec<String>>();
        pairs.sort();
        return format!("{{{}}}", pairs.join(", "));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::lexer::{Token, TokenType, TokenValue};

    use super::AstPrinter;
    use super::BinaryExpr;
    use super::Expr;
    use super::UnaryExpr;

    fn _compare_printed(exprstr: String, expected: String) {
        let lexer = Lexer::new(exprstr);
        let mut start = Instant::now();
        let tokens = lexer.tokenize().unwrap();
        let mut time = start.elapsed();
        println!("Time to lex: {:?}", time);
        let mut parser = Parser::new(tokens);
        start = Instant::now();
        let expr = parser.expression().unwrap();
        time = start.elapsed();
        println!("Time to parse: {:?}", time);
        let printer = AstPrinter {};
        let actual = expr.accept(&printer);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_ast_pretty_print() {
        let printer = AstPrinter {};
        let expr = Expr::Binary(BinaryExpr {
            left: Box::new(Expr::Unary(UnaryExpr {
                op: Token {
                    token_type: TokenType::Plus,
                    lexeme: "+".to_string(),
                    literal: None,
                },
                right: Box::new(Expr::Literal(Token {
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
            right: Box::new(Expr::Grouping(Box::new(Expr::Literal(Token {
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
}
