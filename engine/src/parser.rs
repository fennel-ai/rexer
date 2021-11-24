/// StarQL Grammar:
///
/// TODO: add tuples and lists. Also, add bool support.
/// expression := term
/// term := factor (("+" | "-") factor)*
/// factor := unary (("*" | "/") unary)*
/// unary := primary | "-" unary
/// primary := NUMBER | STRING | "(" expression ")"
///
/// To add:
/// * Tuples
/// * Lists
/// * Booleans, conditionals and control flow
/// * assignment and variables
/// * operator calls
///
use crate::lexer::{Token, TokenType};

pub struct Parser {
    tokens: Vec<Token>,
    previous: Option<Token>,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser {
            tokens: tokens,
            previous: None,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(0)
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
            self.previous = Some(self.tokens.remove(0));
        }
    }

    fn matches(&mut self, token_types: Vec<TokenType>) -> bool {
        for t in token_types {
            if self.check(t) {
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
        let t = self.term();
        t
    }

    fn term(&mut self) -> anyhow::Result<Expr> {
        let mut f = self.factor()?;
        while self.matches(vec![TokenType::Plus, TokenType::Minus]) {
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
        while self.matches(vec![TokenType::Star, TokenType::Slash]) {
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
        if self.matches(vec![TokenType::Minus]) {
            let op = self.previous().unwrap();
            let right = self.unary()?;
            return Ok(Expr::Unary(UnaryExpr {
                op: op,
                right: Box::new(right),
            }));
        }
        self.primary()
    }

    fn primary(&mut self) -> anyhow::Result<Expr> {
        if self.matches(vec![TokenType::LeftParen]) {
            let e = self.expression();
            self.consume(TokenType::RightParen)?;
            return e;
        } else if self.matches(vec![TokenType::Number, TokenType::String]) {
            return Ok(Expr::Literal(
                self.previous()
                    .ok_or(anyhow::anyhow!("previous value should not be none"))?,
            ));
        } else {
            anyhow::bail!("Unexpcted token: {:?}", self.peek())
        }
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
        }
    }
}

trait Visitor<T> {
    fn visit_binary(&self, binary: &BinaryExpr) -> T;
    fn visit_grouping(&self, inner: &Expr) -> T;
    fn visit_unary(&self, unary: &UnaryExpr) -> T;
    fn visit_literal(&self, literal: &Token) -> T;
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
}

#[cfg(test)]
mod tests {
    use crate::lexer::{Token, TokenType, TokenValue};

    use super::AstPrinter;
    use super::BinaryExpr;
    use super::Expr;
    use super::UnaryExpr;

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
        let expr = "1 + 2 * 3".to_string();
        let lexer = Lexer::new(expr);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let expr = parser.expression().unwrap();
        let printer = AstPrinter {};
        let actual = expr.accept(&printer);
        let expected = "(+ 1 (* 2 3))";
        assert_eq!(actual, expected)
    }
}
