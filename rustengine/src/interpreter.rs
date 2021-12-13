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

*/

impl Interpreter {
    fn interpret(query: &str) -> Option<Value> {
        let lexer = Lexer::new(&query);
        let lex_result = lexer.tokenize();
        if let Err(e) = &lex_result {
            eprintln!("{}", e);
            return None;
        }
        let tokens = lex_result.unwrap();
        let mut parser = Parser::new(tokens);
        let parse_result = parser.parse();
        if let Err(e) = &parse_result {
            eprintln!("{}", e);
            return None;
        }
        let ast = parse_result.unwrap();
        // let evaler = Eval::new();
        // ast.accept(&evaler)
        Some(Value::Number(1.0))
    }
}
