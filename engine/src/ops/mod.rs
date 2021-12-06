/*
 * Notes:
 *  1. Every operator should have a signature that describes parameters, types,
 *     default values, return type etc. See if we can make some fields
 *     placeholders and auto-infer them at request time
 *  2. Operators should declare if they are pure or have side-effects. This can
 *     help during query rewriting phase.
 *  3. Every operator has input variable on which they can pull to get more
 *     items. Each call to pull returns the list element as well as a param
 *     struct
 *  4. Every operator should have some sort of internal state. We can use it in
 *     two ways:
 *          a) this will enable to reuse the same code to register different
 *             operators (e.g. with different data or init conditions etc.).
 *          b) Some of this state may also reprent config like objects (e.g.
 *             server address) which can be be filled at runtime by init code
 *  5. Call to any existing operator within rust land is by op.call(name, params)
 *  6. Initing any new Value object requires initing with some existing tags.
 *     Like:   out = new Value(in.tags). Something like this will allow us to
 *     ensure that we can propagate tags easily
 *  7. Every operator can basically destroy/mutate its inputs (and our runtime
 *     makes copies wherever needed to enforce correctness). Non-primitive
 *     values (e.g. lists, records, tables) provide good default in-place
 *     modification methods as needed.
 *
 *
 * TODO: can we re-write value/type to avoid heap boxes?
 *  When/how do we register ops?
 *  When/how do we "init" ops? (i.e. init fields of structs if present)
 *  How do people create Values? (as in what syntax forces them to init tags from existing values?)
 *  Should Value be a trait instead of an enum?
 *
 */

use std::borrow::Borrow;
use std::collections::HashMap;
use std::ffi::VaList;

use crate::ast::eval::Eval;
use crate::ast::Ast;
use crate::types::{Type, Value};
pub mod stdops;

pub struct ParamList<'q, T> {
    args: Vec<(&'q str, T)>,
}

impl<'q, T> ParamList<'q, T> {
    pub fn new(args: Vec<(&'q str, T)>) -> Self {
        ParamList { args }
    }
    pub fn push(&mut self, name: &'q str, value: T) {
        self.args.push((name, value))
    }
    pub fn get(&self, name: &str) -> Option<&T> {
        for (argname, argval) in &self.args {
            if name.eq(*argname) {
                return Some(argval);
            }
        }
        None
    }
}
pub enum RawValue<'q> {
    Evaled(Value),
    Unevaled(Ast<'q>),
}

pub type ParamValueList<'q> = ParamList<'q, &'q Value>;
pub type ParamTypeList<'p> = ParamList<'p, &'p Type>;
pub type ParamRawValueList<'p> = ParamList<'p, RawValue<'p>>;
type Args<'p> = (Value, ParamValueList<'p>);

// TODO: introduce concept of placeholder types
// TODO: introduce concept of documentation for each arg
pub struct Signature<'p> {
    namespace: String,
    name: String,
    docstring: String,
    input_t: Type,
    params_t: ParamTypeList<'p>,
    return_t: Type,
    pure: bool,
}

const BATCH_SIZE: u32 = 64;

pub struct Pipe<'p, 'q> {
    input: Vec<Value>,
    params: ParamRawValueList<'p>,
    evaler: Eval<'q>,
    cache: Vec<Value>,
}
impl<'p, 'q> Pipe<'p, 'q> {
    pub fn new(mut input: Vec<Value>, params: ParamRawValueList<'p>, evaler: Eval<'q>) -> Self {
        input.reverse();
        Pipe {
            input,
            params,
            evaler,
            cache: vec![],
        }
    }

    fn store(&mut self, value: Value) -> &Value {
        let idx = self.cache.len();
        self.cache.push(value);
        &self.cache[idx]
    }

    fn _pull(&mut self, batch: u32) -> anyhow::Result<Vec<Args<'p>>> {
        let mut taken = 0_u32;
        let mut ret: Vec<Args<'p>> = vec![];
        while self.input.len() > 0 && taken < batch {
            taken += 1;

            let elem = self.input.pop().unwrap();

            // TODO: fix this and also reset this after evals
            // self.evaler.environment.define("@", elem);
            let mut pv = ParamValueList::new(vec![]);
            for (paramname, rawvalue) in self.params.args {
                let paramvalue = match rawvalue {
                    RawValue::Evaled(ev) => ev,
                    RawValue::Unevaled(ast) => ast.accept(&mut self.evaler)?,
                };
                let ptr = self.store(paramvalue);
                pv.push(paramname, ptr);
            }
            ret.push((elem, pv));
        }
        Ok(ret)
    }
    pub fn pull(&'q mut self) -> anyhow::Result<Vec<Args>> {
        self._pull(BATCH_SIZE)
    }

    pub fn pull_single(&mut self) -> anyhow::Result<Option<Args>> {
        let mut row = self._pull(1)?;
        Ok(row.pop())
    }
    pub fn pull_all(&'q mut self) -> anyhow::Result<Vec<Args>> {
        let batch = self.input.len() as u32;
        self._pull(batch)
    }
    // TODO: figure out push
    // should inpipe and outpipe be different structs?
    pub fn push(&self, value: Value) {}

    pub fn is_empty(&self) -> bool {
        self.input.is_empty()
    }
}
// TODO: add new or init here for periodic refresh of struct data
pub trait Operator<'p, 'q> {
    fn signature(&self) -> Signature;
    fn run(&self, input: &'q mut Pipe<'p, 'q>, output: &'q mut Pipe<'p, 'q>) -> anyhow::Result<()>;
}

// struct Registry {
//     entries: HashMap<String, Box<dyn Operator>>,
// }

// impl Registry {
//     fn set(&mut self, namespace: String, name: String, op: Box<dyn Operator>) {
//         let k = self.key(namespace, name);
//         self.entries.insert(k, op);
//     }

//     fn get(&self, namespace: String, name: String) -> Option<&Box<dyn Operator>> {
//         let k = self.key(namespace, name);
//         self.entries.get(&k)
//     }
//     fn key(&self, namespace: String, name: String) -> String {
//         namespace + "::" + &name
//     }
// }
