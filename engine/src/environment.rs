use crate::types::Value;
use std::collections::HashMap;

pub struct Environment<'a> {
    table: HashMap<&'a str, Value>,
}

impl<'a> Environment<'a> {
    pub fn new() -> Self {
        let table = HashMap::new();
        Environment { table }
    }

    pub fn define(&mut self, name: &'a str, value: Value) -> anyhow::Result<()> {
        if let Some(_) = self.table.get(name) {
            anyhow::bail!("can not re-define variable: '{}'", name)
        } else {
            self.table.insert(name, value);
        }
        Ok(())
    }

    pub fn get(&self, name: &'a str) -> anyhow::Result<&Value> {
        if let Some(v) = self.table.get(name) {
            Ok(v)
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
        let mut env = Environment::new();
        // getting a name without setting should return error
        assert!(matches!(env.get(name), Err(_)));

        env.define(name, Value::Number(1.0)).unwrap();
        let res = env.get(name).unwrap();
        assert_eq!(*res, Value::Number(1.0));

        assert!(matches!(env.define(name, Value::Number(1.0)), Err(_)));
    }
}
