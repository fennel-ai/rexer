#![allow(dead_code)]
mod ast;
mod compiler;
mod environment;
mod interpreter;
mod lexer;
mod parser;
mod types;
mod vm;

/* TODOs:
    Replace anyhow::Result with own error classes for parseerror & runtime error
    Prettier error printing, clean messages from every function
    Performance work on lexer, parser
*/
