#[derive(Clone)]
pub enum Value {
    Int(i32),
    Double(f64),
    String(String),
    List(&'static [Value]),
}
pub(crate) enum OpCode {
    Return,
    Call,
    LoadConstant,
    LoadVariable,
    AssignVariable,
}

pub(crate) enum Code {
    Op(OpCode),
    Data(u8),
}
