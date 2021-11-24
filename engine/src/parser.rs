use crate::lexer::Token;

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
            binary.op.lexene(),
            binary.left.accept(self),
            binary.right.accept(self)
        );
    }
    fn visit_grouping(&self, inner: &Expr) -> String {
        return format!("(group {})", inner.accept(self));
    }
    fn visit_unary(&self, unary: &UnaryExpr) -> String {
        return format!("({} {})", unary.op.lexene(), unary.right.accept(self));
    }

    fn visit_literal(&self, token: &Token) -> String {
        return token.lexene();
    }
}

#[cfg(test)]
mod tests {
    use crate::lexer::Token;

    use super::AstPrinter;
    use super::BinaryExpr;
    use super::Expr;
    use super::UnaryExpr;

    #[test]
    fn test_ast_pretty_print() {
        let printer = AstPrinter {};
        let expr = Expr::Binary(BinaryExpr {
            left: Box::new(Expr::Unary(UnaryExpr {
                op: Token::Plus,
                right: Box::new(Expr::Literal(Token::Number(123 as f64))),
            })),
            op: Token::Pipe,
            right: Box::new(Expr::Grouping(Box::new(Expr::Literal(Token::Number(
                45.67,
            ))))),
        });
        let actual = expr.accept(&printer);
        let expected = "(| (+ 123) (group 45.67))";
        assert_eq!(actual, expected)
    }
}
