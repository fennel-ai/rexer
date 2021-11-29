use crate::ast::eval::Eval;
use crate::lexer::Lexer;
use crate::parser::Parser;
use crate::types::Value;
struct Interpreter {}

/*
   query owned by Interpreter
   Lexer borrows query from Interpreter and returns Tokens
   Tokens owned by Interpreter
   Parser borrows tokens from Interpreter and returns AST for one statement at a time
   AST is owned by Interpreter
   Evaler borrows AST from Interpreter, computes values (possibly storing some in Environment temporarily) and returns Values
   Interpreter owns Values

   Error reporting is always done by interpreter

   parse_statement() {}
   eval_statement() {}

   parser::parse() {
       while True:
            try:
                parse_statement()
            catch:
                spit out errors
                reorient itself
                continue
   }

   parser::eval () {
       while True:
            try:
                eval_statement()
            catch:
                spit out errors
                abort
   }
*/

impl Interpreter {
    fn interpret(query: &str) -> anyhow::Result<Value> {
        let lexer = Lexer::new(&query);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let ast = parser.parse()?;
        let evaler = Eval::new();
        ast.accept(&evaler)
    }
}
