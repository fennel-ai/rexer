use super::{Operator, ParamTypeList, ParamValueList, InPipe, Signature};
use crate::types::{Type, Value};
pub struct FirstOperator {}

impl<'a> Operator for FirstOperator {
    fn signature(&self) -> Signature {
        Signature {
            namespace: "std".to_string(),
            name: "first".to_string(),
            docstring: "Takes the first element of input list".to_string(),
            input_t: Type::List(Box::new(Type::Any)),
            params_t: ParamTypeList::new(vec![]),
            return_t: Type::List(Box::new(Type::Any)),
            pure: true,
        }
    }
    fn run(&self, input: &InPipe, output: &InPipe) -> anyhow::Result<()> {
        if let Some((v, _)) = input.pull_single()? {
            output.push(v);
            Ok(())
        } else {
            anyhow::bail!("Bad error")
        }
    }
}

pub struct FilterOperator {}

impl Operator for FilterOperator {
    fn signature(&self) -> Signature {
        Signature {
            namespace: "std".to_string(),
            name: "filter".to_string(),
            docstring: "Filters list to only those elements that return True for 'where'"
                .to_string(),
            input_t: Type::List(Box::new(Type::Any)),
            params_t: ParamTypeList::new(vec![("where", &Type::Bool)]),
            return_t: Type::List(Box::new(Type::Any)),
            pure: true,
        }
    }
    fn run(&self, input: &InPipe, output: &InPipe) -> anyhow::Result<()> {
        while let Some((v, params)) = input.pull_single()? {
            if let Some(Value::Bool(true)) = params.get("where") {
                output.push(v);
            } else {
                return anyhow::bail!("bad input");
            }
        }
        Ok(())
    }
}
