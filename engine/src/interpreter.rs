use crate::ast::eval::Eval;
use crate::lexer::Lexer;
use crate::parser::Parser;
use crate::types::Value;
struct Interpreter {}

impl Interpreter {
    fn interpret(query: String) -> anyhow::Result<Value> {
        let lexer = Lexer::new(query);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let ast = parser.parse()?;
        let evaler = Eval {};
        ast.accept(&evaler)
    }
}
