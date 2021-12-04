/*
    TODO: we are currently cloning Value before returning it back from env
    ideally we will return value references, which will likely require us to
    use Arc<Value> everywhere. That is going to be a major refactor touching
    all of Eval and Value definitions themselves (e.g. will Value::List
    constructor operator on list of Values or list of Arc<Value>)
*/
use crate::types::Value;
use std::collections::HashMap;

pub struct Environment<'query> {
    table: HashMap<&'query str, Value>,
    parent: Option<&'query Environment<'query>>,
}

impl<'query> Environment<'query> {
    pub fn new(parent: Option<&'query Environment<'query>>) -> Self {
        let table = HashMap::new();
        Environment { table, parent }
    }

    pub fn define(&mut self, name: &'query str, value: Value) -> anyhow::Result<()> {
        if let Some(_) = self.table.get(name) {
            anyhow::bail!("can not re-define variable: '{}'", name)
        } else {
            self.table.insert(name, value);
        }
        Ok(())
    }

    pub fn get(&self, name: &'query str) -> anyhow::Result<Value> {
        if let Some(v) = self.table.get(name) {
            let r = v.clone();
            Ok(r)
        } else if let Some(env) = self.parent {
            // if table doesn't have this symbol, check in its parent if present
            env.get(name)
        } else {
            anyhow::bail!("accessing undefined variable: '{}'", name)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Environment;
    use crate::types::Value;

    #[test]
    fn test_get_set() {
        let name = "hi";
        let mut env = Environment::new(None);
        // getting a name without setting should return error
        assert!(matches!(env.get(name), Err(_)));

        env.define(name, Value::Number(1.0)).unwrap();
        let res = env.get(name).unwrap();
        assert_eq!(res, Value::Number(1.0));

        assert!(matches!(env.define(name, Value::Number(1.0)), Err(_)));
    }
    #[test]
    fn test_scop() {
        let name1 = "hi";
        let name2 = "bye";
        let name3 = "okay";
        let mut global = Environment::new(None);
        global.define(name1, Value::Number(1.0)).unwrap();
        global.define(name2, Value::Number(2.0)).unwrap();
        assert_eq!(global.get(name1).unwrap(), Value::Number(1.0));
        assert_eq!(global.get(name2).unwrap(), Value::Number(2.0));

        let mut local = Environment::new(Some(&global));
        local.define(name1, Value::Bool(false)).unwrap();
        assert_eq!(local.get(name1).unwrap(), Value::Bool(false));
        assert_eq!(local.get(name2).unwrap(), Value::Number(2.0));
        local.define(name3, Value::Number(3.0)).unwrap();
        assert_eq!(local.get(name3).unwrap(), Value::Number(3.0));
    }
}
