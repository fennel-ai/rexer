use super::compiler::Tape;
use super::types::*;
use std::collections::HashMap;

struct Runtime {
    tape: Tape,
    ip: usize,
    stack: Vec<&'static Value>,
    symbol_table: HashMap<String, Value>,
}

impl Runtime {
    pub fn new(t: Tape) -> Self {
        Runtime {
            tape: t,
            ip: 0,
            stack: vec![],
            symbol_table: HashMap::new(),
        }
    }

    fn advance(&mut self) -> &Code {
        let code = self.tape.code_at(self.ip);
        self.ip += 1;
        code
    }

    fn read_usize(&mut self) -> usize {
        // TODO: handle errors.
        self.tape.read_usize(self.ip).unwrap()
    }

    // TODO(abhay): Use correct error type.
    pub fn run(&mut self) -> std::io::Result<Value> {
        loop {
            let code = self.advance();
            match code {
                Code::Data(_) => {
                    // TODO: return error.
                    panic!("got data; expected op");
                }
                Code::Op(opcode) => match opcode {
                    OpCode::LoadConstant => {
                        // TODO: handle error.
                        let idx = self.read_usize();
                        // self.stack.push(&self.tape.constants[idx]);
                    }
                    _ => {
                        unimplemented!("not implemented");
                    }
                },
            }
        }
    }
}
