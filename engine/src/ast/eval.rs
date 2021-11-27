use super::{Ast, OpCall, Visitor};
use crate::lexer::{Token, TokenType};
use crate::types::Value;
use std::collections::HashMap;

struct Eval {}

// impl Visitor<Value> for Eval {
//     fn visit_literal(&self, token: &Token) -> Value {
//         if let Some(l) = token.literal {
//             match l {
//                 TokenValue::Double(d) => Value::Number(d),
//                 TokenValue::String(s) => {
//                     Value::String(s),
//                 }
//                 _ => unimplemented!()
//             }
//     }
//     fn visit_binary(&self, left: &Ast, op: &Token, right: &Ast) -> Value {}
//     fn visit_grouping(&self, inner: &Ast) -> Value {}
//     fn visit_list(&self, list: &[Ast]) -> Value {}
//     fn visit_opexp(&self, root: &Ast, opcalls: &[OpCall]) -> Value {}
//     fn visit_query(&self, statements: &[Ast]) -> Value {}
//     fn visit_record(&self, record: &HashMap<String, Ast>) -> Value {}
//     fn visit_statement(&self, variable: &Option<String>, body: &Ast) -> Value {}
//     fn visit_unary(&self, op: &Token, right: &Ast) -> Value {}
// }
