use std::collections::HashMap;
use std::fmt;
use std::ops;

#[derive(Debug, Clone)]
pub struct ParseError {
    pub line: u32,
    pub message: String,
}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[line {}] error: {}", self.line, self.message)
    }
}
pub type ParseResult<T> = std::result::Result<T, ParseError>;

#[derive(Debug, Clone)]
struct RuntimeError;
type RuntimeResult<T> = std::result::Result<T, RuntimeError>;

#[derive(Clone, Debug, PartialEq)]
// TODO(nikhil): convert record to use two vectors instead of hashmap
pub enum Type {
    Any,
    Number,
    String,
    Bool,
    List(Box<Type>),
    Record(HashMap<String, Box<Type>>),
}

#[derive(Debug)]
pub enum Value {
    Number(f64),
    String(String),
    Bool(bool),
    List(Vec<Value>),
    Record(HashMap<String, Box<Value>>),
}

impl ops::Add for Value {
    type Output = anyhow::Result<Value>;

    fn add(self, rhs: Value) -> anyhow::Result<Value> {
        match (self, rhs) {
            (Value::Number(l), Value::Number(r)) => Ok(Value::Number(l + r)),
            (Value::String(l), Value::String(r)) => Ok(Value::String(l + &r)),
            _ => anyhow::bail!("plus operator only defined on strings and numbers"),
        }
    }
}

impl ops::Sub for Value {
    type Output = anyhow::Result<Value>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Number(l), Value::Number(r)) => Ok(Value::Number(l - r)),
            _ => anyhow::bail!("minus operator only defined on numbers"),
        }
    }
}

impl ops::Mul for Value {
    type Output = anyhow::Result<Value>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Number(l), Value::Number(r)) => Ok(Value::Number(l * r)),
            _ => anyhow::bail!("multiplication operator only defined on numbers"),
        }
    }
}

impl ops::Div for Value {
    type Output = anyhow::Result<Value>;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Number(l), Value::Number(r)) => Ok(Value::Number(l / r)),
            _ => anyhow::bail!("division operator only defined on numbers"),
        }
    }
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

// implementing binary operators for Value type.
impl Value {
    pub fn lt(&self, other: &Value) -> anyhow::Result<Value> {
        match (self, other) {
            (Value::Number(l), Value::Number(r)) => Ok(Value::Bool(l < r)),
            _ => anyhow::bail!("< operator only defined on numbers"),
        }
    }
    pub fn le(&self, other: &Value) -> anyhow::Result<Value> {
        match (self, other) {
            (Value::Number(l), Value::Number(r)) => Ok(Value::Bool(l <= r)),
            _ => anyhow::bail!("<= operator only defined on numbers"),
        }
    }
    pub fn gt(&self, other: &Value) -> anyhow::Result<Value> {
        match (self, other) {
            (Value::Number(l), Value::Number(r)) => Ok(Value::Bool(l > r)),
            _ => anyhow::bail!("> operator only defined on numbers"),
        }
    }
    pub fn ge(&self, other: &Value) -> anyhow::Result<Value> {
        match (self, other) {
            (Value::Number(l), Value::Number(r)) => Ok(Value::Bool(l >= r)),
            _ => anyhow::bail!(">= operator only defined on numbers"),
        }
    }
}

impl std::cmp::PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Number(l), Value::Number(r)) => l == r,
            (Value::String(l), Value::String(r)) => l == r,
            (Value::Bool(l), Value::Bool(r)) => l == r,
            (Value::List(l), Value::List(r)) => {
                if l.len() != r.len() {
                    false
                } else {
                    l.iter().zip(r.iter()).all(|(x, y)| x.eq(y))
                }
            }
            (Value::Record(l), Value::Record(r)) => {
                if l.len() != r.len() {
                    false
                } else {
                    l.iter().all(|(k, v)| r.get(k) == Some(v))
                }
            }
            _ => false,
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
