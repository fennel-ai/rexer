use super::{Ast, OpCall, Visitor};
use crate::lexer::{Token, TokenType};

pub struct LocalFinder {}

impl Visitor<'_, bool> for LocalFinder {
    fn visit_atom(&mut self, token: &Token) -> bool {
        println!(
            "Coming to visit atom. Type: {:?}, Literal: {}",
            token.token_type,
            token.literal()
        );
        match token.token_type {
            TokenType::Variable => token.literal() == "@",
            _ => false,
        }
    }

    fn visit_binary(&mut self, left: &Ast, _op: &Token, right: &Ast) -> bool {
        left.accept(self) || right.accept(self)
    }

    fn visit_grouping(&mut self, inner: &Ast) -> bool {
        inner.accept(self)
    }

    fn visit_list(&mut self, list: &[Ast]) -> bool {
        list.iter().any(|ast| ast.accept(self))
    }

    fn visit_opexp(&mut self, root: &Ast, opcalls: &[OpCall]) -> bool {
        if root.accept(self) {
            return true;
        }
        for opcall in opcalls {
            for (_k, ast) in &opcall.args {
                if ast.accept(self) {
                    return true;
                }
            }
        }
        false
    }

    fn visit_query(&mut self, statements: &[Ast]) -> bool {
        statements.iter().any(|ast| ast.accept(self))
    }

    fn visit_record(&mut self, _names: &[Token], values: &[Ast]) -> bool {
        values.iter().any(|v| v.accept(self))
    }

    fn visit_statement(&mut self, _variable: Option<&Token>, body: &Ast) -> bool {
        body.accept(self)
    }

    fn visit_unary(&mut self, _op: &Token, right: &Ast) -> bool {
        right.accept(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::localfinder::LocalFinder;
    use crate::lexer::Lexer;
    use crate::parser::Parser;
    use std::time::Instant;

    fn _compare_printed(exprstr: &str, expected: bool) {
        let lexer = Lexer::new(exprstr);
        let mut start = Instant::now();
        let tokens = lexer.tokenize().unwrap();
        let mut time = start.elapsed();
        println!("Time to lex: {:?}", time);
        let mut parser = Parser::new(tokens);
        start = Instant::now();
        let expr = parser.parse().unwrap();
        time = start.elapsed();
        println!("Time to parse: {:?}", time);
        let mut localfinder = LocalFinder {};
        let actual = expr.accept(&mut localfinder);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_all() {
        _compare_printed("1 + 2 * 3", false);
        _compare_printed("1 + 2 * $hi", false);
        _compare_printed("1 + @ * 3", true);
        _compare_printed("1 + @ * $hi", true);
        _compare_printed("[0, $abc, 1 + @ * $hi]", true);
        _compare_printed("[0, $abc, 1 + @ * $hi] | a.b(hi=1)", true);
        _compare_printed("[0, $abc, 1 + $hi] | a.b(hi=1)", false);
        // TODO: enable this test after adding support for property lookups
        // _compare_printed("[0, $abc, 1 + $hi] | a.b(hi=@.x)", true);
    }
}
