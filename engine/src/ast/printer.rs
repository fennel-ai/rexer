use super::{Ast, OpCall, Visitor};
use crate::lexer::Token;
use itertools::Itertools;
use std::collections::HashMap;
use std::fmt;
struct Printer {}

impl fmt::Display for Ast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.accept(&Printer {}))
    }
}

impl Visitor<String> for Printer {
    fn visit_binary(&self, left: &Ast, op: &Token, right: &Ast) -> String {
        return format!(
            "({} {} {})",
            op.lexeme,
            left.accept(self),
            right.accept(self)
        );
    }
    fn visit_grouping(&self, inner: &Ast) -> String {
        return format!("(group {})", inner.accept(self));
    }
    fn visit_unary(&self, op: &Token, right: &Ast) -> String {
        return format!("({} {})", op.lexeme, right.accept(self));
    }

    fn visit_atom(&self, token: &Token) -> String {
        return token.lexeme.clone();
    }

    fn visit_list(&self, list: &[Ast]) -> String {
        return format!("[{}]", list.iter().map(|e| e.accept(self)).join(", "));
    }

    fn visit_record(&self, record: &HashMap<Token, Ast>) -> String {
        format!(
            "{{{}}}",
            record
                .iter()
                .map(|(k, v)| format!("{}={}", k.lexeme, v.accept(self)))
                .sorted()
                .join(", ")
        )
    }
    fn visit_opexp(&self, root: &Ast, opcalls: &[OpCall]) -> String {
        if opcalls.len() == 0 {
            root.accept(self)
        } else {
            let opcallstr = opcalls
                .iter()
                .map(|opcall| {
                    format!(
                        "{}({})",
                        opcall.path.iter().map(|t| &t.lexeme).join("."),
                        opcall
                            .args
                            .iter()
                            .map(|(k, v)| format!("{}={}", k.lexeme, v.accept(self)))
                            .sorted()
                            .join(", ")
                    )
                })
                .join(" | ");
            format!("{} | {}", root.accept(self), opcallstr)
        }
    }
    fn visit_statement(&self, variable: &Option<Token>, body: &Ast) -> String {
        let assignment = if let Some(s) = variable {
            format!("{} = ", s.lexeme)
        } else {
            "".to_string()
        };
        format!("{}{}", assignment, body.accept(self))
    }
    fn visit_query(&self, query: &[Ast]) -> String {
        query.iter().map(|s| s.accept(self)).join(";\n")
    }
}

#[cfg(test)]
mod tests {
    use super::Printer;
    use crate::lexer::Lexer;
    use crate::parser::Parser;
    use std::time::Instant;

    fn _compare_printed(exprstr: String, expected: String) {
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
        let printer = Printer {};
        let actual = expr.accept(&printer);
        assert_eq!(actual, expected);
    }

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
