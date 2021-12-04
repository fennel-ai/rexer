#![allow(dead_code)]
mod ast;
mod compiler;
mod environment;
// mod interpreter;
mod lexer;
// mod ops;
mod parser;
mod types;
mod vm;

/* TODOs:
    Error mechanics -- who catches/prints errors, what info do we show and how do
        tie it with Python calls?
    Replace anyhow::Result with own error classes for parseerror & runtime error
    Prettier error printing, clean messages from every function
    Parser should just own Lexer?
    Return tokens one by one
    Performance work on lexer, parser
*/
