// /// StarQL Grammar:
// ///
// /// TODO: add tuples and lists. Also, add bool support.
// /// expression := term
// /// term := factor (("+" | "-") factor)*
// /// factor := unary (("*" | "/") unary)*
// /// unary := primary | "-" unary
// /// literal := NUMBER | STRING
// /// primary := literal | "(" expression ")"
// ///
// /// To add:
// /// * Tuples
// /// * Lists
// /// * Booleans, conditionals and control flow
// /// * assignment and variables
// /// * operator calls
// ///
// use super::lexer::TokenType;
// use super::types::*;
// use anyhow::anyhow;
// use anyhow::Result;

// use std::collections::HashMap;
// pub struct Tape {
//     pub(crate) codes: Vec<Code>,
//     pub(crate) constants: Vec<Value>,
//     lines: Vec<usize>,
// }

// impl Tape {
//     pub fn new() -> Tape {
//         Tape {
//             codes: vec![],
//             constants: vec![],
//             lines: vec![],
//         }
//     }

//     fn emit(&mut self, c: Code) {
//         self.codes.push(c);
//     }

//     fn emit_bytes(&mut self, bytes: &[u8]) {
//         for b in bytes.to_owned() {
//             self.codes.push(Code::Data(b));
//         }
//     }

//     pub(crate) fn code_at(&self, ip: usize) -> &Code {
//         &self.codes[ip]
//     }

//     pub(crate) fn write_usize(&mut self, mut v: usize) {
//         for _ in 0..std::mem::size_of::<usize>() {
//             self.codes.push(Code::Data((v & 0xff) as u8));
//             v = v >> 8;
//         }
//     }

//     pub(crate) fn read_usize(&self, start: usize) -> Result<usize> {
//         let mut v: usize = 0;
//         let mut shift = 0;
//         for i in 0..std::mem::size_of::<usize>() {
//             let code = self.code_at(start + i);
//             if let Code::Data(b) = code {
//                 v += (*b as usize) << shift;
//                 shift += 8;
//             } else {
//                 // TODO: return error
//                 panic!("unexpected value");
//             }
//         }
//         Ok(v)
//     }
// }

// // TODO: do we need a separate parser?
// pub fn compile(tokens: Vec<Token>) -> Tape {
//     let mut t = Tape::new();
//     for token in tokens.into_iter() {
//         match token.token_type {
//             TokenType::Number(n) => {
//                 let idx = t.constants.len();
//                 t.constants.push(Value::Double(n));
//                 t.emit(Code::Op(OpCode::LoadConstant));
//                 // TODO: write varint instead of fixed-size.
//                 t.write_usize(idx);
//             }
//             _ => {
//                 unimplemented!()
//             }
//         }
//     }
//     t
// }

// #[cfg(test)]
// mod test {
//     use super::Tape;
//     #[test]
//     fn test_usize() {
//         let mut t = Tape::new();
//         t.write_usize(1028);
//         assert_eq!(1028, t.read_usize(0).unwrap());
//     }
// }

// struct Parser {
//     tokens: Vec<TokenType>,
//     current: usize,
// }

// impl Parser {
//     pub fn new(mut tokens: Vec<TokenType>) -> Self {
//         tokens.reverse();
//         Parser {
//             current: tokens.len(),
//             tokens: tokens,
//         }
//     }

//     fn peek(&mut self) -> Option<&TokenType> {
//         if self.tokens.len() == 0 {
//             None
//         } else {
//             Some(&self.tokens[self.current])
//         }
//     }

//     fn advance(&mut self) -> Option<TokenType> {
//         if self.tokens.len() == 0 {
//             None
//         } else {
//             let t = self.tokens.pop();
//             self.current -= 1;
//             t
//         }
//     }

//     fn assignment(&mut self, id: String) -> Result<Expr> {
//         // advance over equal sign.
//         if let Some(TokenType::Equal) = self.advance() {
//             let e = self.next();
//             return match e {
//                 Some(expr) => Ok(Expr::Assignment(id, Box::new(expr?))),
//                 None => Err(anyhow!("missing expression after '='")),
//             };
//         }
//         return Err(anyhow!("expected '=' after identifier in assignment"));
//     }

//     pub fn next(&mut self) -> Option<Result<Expr>> {
//         if let Some(t) = self.advance() {
//             let r = match t {
//                 TokenType::Number(n) => Some(Ok(Expr::Number(n))),
//                 TokenType::Identifier(id) => Some(self.assignment(id)),
//                 _ => {
//                     unimplemented!("foo");
//                 }
//             };
//             return r;
//         }
//         None
//     }
// }

// enum Expr {
//     Number(f64),
//     Variable(String),
//     List(Vec<Expr>),
//     Assignment(String, Box<Expr>),
//     OpCall {
//         name: String,
//         input: Box<Expr>,
//         args: HashMap<String, Box<Expr>>,
//     },
// }
