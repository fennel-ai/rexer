use super::{Ast, OpCall, Visitor};
use crate::lexer::{Token, TokenType};
use crate::types::Value;
use anyhow;
use std::collections::HashMap;

/*
Remaining items:
* eval identifier and variable in visit_atom.
* identifier
* variable assignment
* op calls.
* "and" and "or" as binary operators over values.
*/

struct Eval {}

impl Visitor<anyhow::Result<Value>> for Eval {
    fn visit_atom(&self, token: &Token) -> anyhow::Result<Value> {
        let literal = token.literal();
        match token.token_type {
            TokenType::String => Ok(Value::String(literal)),
            TokenType::Number => Ok(Value::Number(literal.parse::<f64>()?)),
            TokenType::Bool => Ok(Value::Bool(literal.parse::<bool>()?)),
            TokenType::Variable => unimplemented!("todo"),
            TokenType::Identifier => unimplemented!("todo"),
            _ => anyhow::bail!("unexpected atom token {:?}", token),
        }
    }
    fn visit_binary(&self, left: &Ast, op: &Token, right: &Ast) -> anyhow::Result<Value> {
        match op.token_type {
            TokenType::Plus => left.accept(self)? + right.accept(self)?,
            TokenType::Minus => left.accept(self)? - right.accept(self)?,
            TokenType::Star => left.accept(self)? * right.accept(self)?,
            TokenType::Slash => left.accept(self)? / right.accept(self)?,
            TokenType::GreaterEqual => left.accept(self)?.ge(&right.accept(self)?),
            TokenType::Greater => left.accept(self)?.gt(&right.accept(self)?),
            TokenType::LesserEqual => left.accept(self)?.le(&right.accept(self)?),
            TokenType::Lesser => left.accept(self)?.lt(&right.accept(self)?),
            TokenType::EqualEqual => Ok(Value::Bool(left.accept(self)? == right.accept(self)?)),
            _ => {
                anyhow::bail!("unexpected binary operator: {:?}", op)
            }
        }
    }
    fn visit_grouping(&self, inner: &Ast) -> anyhow::Result<Value> {
        inner.accept(self)
    }

    fn visit_list(&self, list: &[Ast]) -> anyhow::Result<Value> {
        let mut v = vec![];
        let mut t = None;
        for ast in list {
            let r = ast.accept(self)?;
            if t != None && Some(r.get_type()) != t {
                anyhow::bail!("list type: {:?}, element type: {:?}", t, r.get_type())
            } else {
                t = Some(r.get_type());
                v.push(r);
            }
        }
        Ok(Value::List(v))
    }

    fn visit_opexp(&self, root: &Ast, opcalls: &[OpCall]) -> anyhow::Result<Value> {
        root.accept(self)
        // TODO: implement op
    }

    fn visit_query(&self, statements: &[Ast]) -> anyhow::Result<Value> {
        let mut r = None;
        for ast in statements {
            r = Some(ast.accept(self)?);
        }
        r.ok_or_else(|| anyhow::anyhow!("query has no statements"))
    }

    fn visit_record(&self, record: &HashMap<Token, Ast>) -> anyhow::Result<Value> {
        let mut er = HashMap::with_capacity(record.len());
        for (k, v) in record {
            er.insert(k.literal(), Box::new(v.accept(self)?));
        }
        Ok(Value::Record(er))
    }

    fn visit_statement(&self, variable: &Option<Token>, body: &Ast) -> anyhow::Result<Value> {
        // TODO: assign value to variable in symbol table.
        body.accept(self)
    }

    fn visit_unary(&self, op: &Token, right: &Ast) -> anyhow::Result<Value> {
        match (op.token_type, right.accept(self)?) {
            (TokenType::Minus, Value::Number(n)) => Ok(Value::Number(-n)),
            _ => anyhow::bail!("unaccepted unary call: op {:?} operand {}", op, right),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Eval;
    use crate::lexer::Lexer;
    use crate::parser::Parser;
    use crate::types::Value;
    use std::time::Instant;

    fn expect(exprstr: String, expected: Value) {
        let lexer = Lexer::new(exprstr);
        let mut start = Instant::now();
        let tokens = lexer.tokenize().unwrap();
        let mut time = start.elapsed();
        println!("Time to lex: {:?}", time);
        let mut parser = Parser::new(tokens);
        start = Instant::now();
        let ast = parser.parse().unwrap();
        time = start.elapsed();
        println!("Time to parse: {:?}", time);

        start = Instant::now();
        let e = &Eval {};
        let actual = ast.accept(e).unwrap();
        time = start.elapsed();
        println!("Time to eval: {:?}", time);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_add() {
        expect("2 + 3".to_string(), Value::Number(5.0))
    }

    #[test]
    fn test_query() {
        expect("2 + 3 == 5; ".to_string(), Value::Bool(true));
        expect("2 + 3 == 5; false".to_string(), Value::Bool(false));
    }
}
