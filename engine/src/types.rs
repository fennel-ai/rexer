use std::collections::HashMap;

#[derive(Clone)]

pub enum Type {
    Any,
    Number,
    String,
    Bool,
    List(Box<Type>),
    Record(HashMap<String, Box<Type>>),
}

pub enum Value {
    Number(f64),
    String(String),
    Bool(bool),
    List(Vec<Value>),
    Record(HashMap<String, Box<Value>>),
}

impl Value {
    pub fn get_type(&self) -> Type {
        match self {
            Value::Number(_) => Type::Number,
            Value::String(_) => Type::String,
            Value::Bool(_) => Type::Bool,
            Value::List(l) => {
                if l.len() == 0 {
                    Type::Any
                } else {
                    Type::List(Box::new(l[0].get_type()))
                }
            }
            Value::Record(r) => {
                let mut h = HashMap::new();
                for (k, v) in r.iter() {
                    h.insert(k.clone(), Box::new(v.get_type()));
                }
                Type::Record(h)
            }
        }
    }
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
