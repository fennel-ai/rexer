use super::{Ast, OpCall, Visitor};
use crate::lexer::Token;
use itertools::Itertools;
use std::fmt;
struct Printer {}

impl fmt::Display for Ast<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.accept(&mut Printer {}))
    }
}

impl Visitor<'_, String> for Printer {
    fn visit_binary(&mut self, left: &Ast, op: &Token, right: &Ast) -> String {
        return format!(
            "({} {} {})",
            op.lexeme,
            left.accept(self),
            right.accept(self)
        );
    }
    fn visit_grouping(&mut self, inner: &Ast) -> String {
        return format!("(group {})", inner.accept(self));
    }
    fn visit_unary(&mut self, op: &Token, right: &Ast) -> String {
        return format!("({} {})", op.lexeme, right.accept(self));
    }

    fn visit_atom(&mut self, token: &Token) -> String {
        token.lexeme.to_string()
    }

    fn visit_list(&mut self, list: &[Ast]) -> String {
        return format!("[{}]", list.iter().map(|e| e.accept(self)).join(", "));
    }

    fn visit_record(&mut self, names: &[Token], values: &[Ast]) -> String {
        format!(
            "{{{}}}",
            names
                .iter()
                .enumerate()
                .map(|(i, n)| format!("{}={}", n.lexeme, values[i].accept(self)))
                .sorted()
                .join(", ")
        )
    }

    fn visit_opexp(&mut self, root: &Ast, opcalls: &[OpCall]) -> String {
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
    fn visit_statement(&mut self, variable: Option<&Token>, body: &Ast) -> String {
        let assignment = if let Some(s) = variable {
            format!("{} = ", s.lexeme)
        } else {
            "".to_string()
        };
        format!("{}{}", assignment, body.accept(self))
    }
    fn visit_query(&mut self, query: &[Ast]) -> String {
        query.iter().map(|s| s.accept(self)).join(";\n")
    }
}

#[cfg(test)]
mod tests {
    use super::Printer;
    use crate::lexer::Lexer;
    use crate::parser::Parser;
    use std::time::Instant;

    fn _compare_printed(exprstr: &str, expected: String) {
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
        let mut printer = Printer {};
        let actual = expr.accept(&mut printer);
        assert_eq!(actual, expected);
    }

    #[test]
    fn end_to_end() {
        let exprstr = "1 + 2 * 3";
        let expected = "(+ 1 (* 2 3))".to_string();
        _compare_printed(exprstr, expected);
    }

    #[test]
    fn with_booleans() {
        let exprstr = "2 >= 3 or true and 1 == 2";
        let expected = "(or (>= 2 3) (and true (== 1 2)))".to_string();
        _compare_printed(exprstr, expected);
    }

    #[test]
    fn parse_list() {
        let exprstr = "[1,2,3]";
        let expected = "[1, 2, 3]".to_string();
        _compare_printed(exprstr, expected)
    }

    #[test]
    fn parse_record() {
        let first = "{xyz=123, foo=\"bar\"}";
        let second = "{foo=\"bar\",xyz=123}";
        let expected = "{foo=\"bar\", xyz=123}".to_string();
        _compare_printed(first, expected.clone());
        _compare_printed(second, expected);
    }

    #[test]
    fn parse_opexp() {
        let expstr = "12 | a.b.c(x=123, y=\"hi\",)";
        let expected = "12 | a.b.c(x=123, y=\"hi\")".to_string();
        _compare_printed(expstr, expected);
    }

    #[test]
    fn parse_statement() {
        let expstr = "name = 12 | a.b.c(x=123, y=\"hi\",)";
        let expected = "name = 12 | a.b.c(x=123, y=\"hi\")".to_string();
        _compare_printed(expstr, expected);
        _compare_printed("5", "5".to_string());
    }
    #[test]
    fn parse_program() {
        let expstr = "name = 12 | a.b.c(x=123, y=\"hi\",); abc = 8; 5;";
        let expected = "name = 12 | a.b.c(x=123, y=\"hi\");\nabc = 8;\n5".to_string();
        _compare_printed(expstr, expected);
    }

    #[test]
    fn parse_underscores() {
        let exprstr = "ab_bc1 = 1; _bc = 5";
        let expected = "ab_bc1 = 1;\n_bc = 5".to_string();
        _compare_printed(exprstr, expected)
    }
    #[test]
    fn parse_comments() {
        let exprstr = "// this is commment;\nx = 5; $x";
        let expected = "x = 5;\n$x".to_string();
        _compare_printed(exprstr, expected)
    }
    #[test]
    fn parse_local_closure() {
        let exprstr = "[0, 1, 2] | a.b(where=@)";
        let expected = exprstr.to_string();
        _compare_printed(exprstr, expected)
    }
    #[test]
    fn parse_large_list_of_records() {
        let numstr = (0..10000)
            .map(|i: i32| format!("{{x={}, y={}}}", i, i))
            .collect::<Vec<String>>()
            .join(", ");
        let expstr = format!("[{}]", numstr);
        let expected = expstr.clone();
        _compare_printed(&expstr, expected);
    }
}
