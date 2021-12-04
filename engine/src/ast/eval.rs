use super::localfinder::LocalFinder;
use super::{Ast, OpCall, Visitor};
use crate::environment::Environment;
use crate::lexer::{Token, TokenType};
// use crate::ops::stdops::{FilterOperator, FirstOperator};
// use crate::ops::{Operator, ParamRawValueList, Pipe, RawValue};
use crate::types::Value;
use anyhow;
use std::collections::HashMap;

/*
Remaining items:
* identifier
* variable assignment
* op calls.
Storage of Value: Options
==========================
1. AST eval returns Arc<Value>


2. Store a Value inside each AST and eval returns &value with lifetime of ast. This can't work due to '@'
3. Store a vector of values inside Eval. eval returns &value with lifetine of eval.
        We store &Value inside hashtable and return a copy of &Value?
        All our functions (e.g. +, -) operate on &Value
        We pass &[Value] to operators
3. Store query, tokens, ASTs inside each interpreter. Each AST stores its own value. This way lifetime of everything is 'q
4. AST returns Value and we do copy wherever needed
        Value of each param will be copied again and again when calling operations
        Getting data from environment will also create copies (but maybe that is okay?)

Representation of Value:
========================
1. Value is an enum with List as a member. We pass everything as List to operators, who have to convert it to Vector on their own
2. Value is a trait which is implemented on Vector<Value> as well. All function signatures are on Value but passed around with Vector<Value>
    One advantage => we can also implement same trait on Arrow objects.
3. Core values are an enum. List is not.

*/

pub struct Eval<'q> {
    environment: Environment<'q>,
}

impl<'q> Eval<'q> {
    pub fn new(environment: Option<Environment<'q>>) -> Self {
        if let Some(env) = environment {
            Self { environment: env }
        } else {
            Self {
                environment: Environment::new(None),
            }
        }
    }
    // pub fn local(&mut self, local: &Value, ast: &Ast<'q>) -> anyhow::Result<Value> {
    //     self.environment.define("@", local);
    //     let ret = ast.accept(self)?;
    //     // TODO: reset @ here
    //     Ok(ret)
    // }
}

impl<'q> Visitor<'q, anyhow::Result<Value>> for Eval<'q> {
    fn visit_atom(&mut self, token: &'q Token) -> anyhow::Result<Value> {
        let literal = token.literal().to_string();
        match token.token_type {
            TokenType::String => Ok(Value::String(literal)),
            TokenType::Number => Ok(Value::Number(literal.parse::<f64>()?)),
            TokenType::Bool => Ok(Value::Bool(literal.parse::<bool>()?)),
            TokenType::Variable => self.environment.get(token.literal()),
            // Note: we should never see an identifier called all the way to
            // visit_atom. If present, a higher level match (e.g. opexp) should
            // handle it
            _ => anyhow::bail!("unexpected atom token {:?}", token),
        }
    }
    fn visit_binary(
        &mut self,
        left: &'q Ast<'q>,
        op: &'q Token<'q>,
        right: &'q Ast<'q>,
    ) -> anyhow::Result<Value> {
        match op.token_type {
            TokenType::Plus => left.accept(self)? + right.accept(self)?,
            TokenType::Minus => left.accept(self)? - right.accept(self)?,
            TokenType::Star => left.accept(self)? * right.accept(self)?,
            TokenType::Slash => left.accept(self)? / right.accept(self)?,
            TokenType::GreaterEqual => left.accept(self)?.ge(&right.accept(self)?),
            TokenType::Greater => left.accept(self)?.gt(&right.accept(self)?),
            TokenType::LesserEqual => left.accept(self)?.le(&right.accept(self)?),
            TokenType::Lesser => left.accept(self)?.lt(&right.accept(self)?),
            TokenType::EqualEqual => Ok(Value::Bool(left.accept(self)? == right.accept(self)?)),
            TokenType::BangEqual => Ok(Value::Bool(!(left.accept(self)? == right.accept(self)?))),
            TokenType::Or => left.accept(self)?.or(&right.accept(self)?),
            TokenType::And => left.accept(self)?.and(&right.accept(self)?),
            _ => {
                anyhow::bail!("unexpected binary operator in eval: {:?}", op)
            }
        }
    }
    fn visit_grouping(&mut self, inner: &'q Ast<'q>) -> anyhow::Result<Value> {
        inner.accept(self)
    }

    fn visit_list(&mut self, list: &'q [Ast<'q>]) -> anyhow::Result<Value> {
        let mut v = vec![];
        let mut t = None;
        for ast in list {
            let r = ast.accept(self)?;
            if t != None && Some(r.get_type()) != t {
                anyhow::bail!(
                    "list of type {:?} can not hold element of type: {:?}",
                    t,
                    r.get_type()
                )
            } else {
                t = Some(r.get_type());
                v.push(r);
            }
        }
        Ok(Value::List(v))
    }

    fn visit_opexp(&mut self, root: &'q Ast, opcalls: &'q [OpCall]) -> anyhow::Result<Value> {
        let mut base = root.accept(self)?;
        // if opcalls.len() == 0 {
        //     return Ok(base);
        // }
        // let mut root = if let Value::List(v) = base {
        //     v
        // } else {
        //     anyhow::bail!("can not apply operators on non-list operand");
        // };
        // // TODO: implement op
        // for opcall in opcalls {
        //     let path = &opcall.path;
        //     // TODO: replace this with some registry lookup
        //     let (namespace, name) = match path.len() {
        //         0 => anyhow::bail!("operator name/path not given"),
        //         1 => ("std", path[0].literal()),
        //         2 => (path[0].literal(), path[1].literal()),
        //         _ => unimplemented!("complex operator paths not supported"),
        //     };
        //     let localfinder = LocalFinder {};
        //     let pvl = ParamRawValueList::new(vec![]);
        //     for (paramtoken, &paramast) in opcall.args.iter() {
        //         let rawvalue = if paramast.accept(&localfinder) {
        //             RawValue::Unevaled(paramast)
        //         } else {
        //             RawValue::Evaled(paramast.accept(self)?)
        //         };
        //         pvl.push(paramtoken.literal(), rawvalue);
        //     }
        //     let pipe = Pipe::new(
        //         root,
        //         pvl,
        //         Eval {
        //             environment: Environment::new(Some(&mut self.environment)),
        //         },
        //     );
        //     let operator: Box<dyn Operator> = match (namespace, name) {
        //         ("std", "filter") => Box::new(FilterOperator {}),
        //         ("std", "first") => Box::new(FirstOperator {}),
        //         _ => unimplemented!(),
        //     };
        //     operator.run(&pipe, &pipe)?;
        // }
        Ok(base)
    }

    fn visit_query(&mut self, statements: &'q [Ast<'q>]) -> anyhow::Result<Value> {
        let mut r = None;
        for ast in statements {
            r = Some(ast.accept(self)?);
        }
        r.ok_or_else(|| anyhow::anyhow!("query has no statements"))
    }

    fn visit_record(
        &mut self,
        names: &'q [Token<'q>],
        values: &'q [Ast<'q>],
    ) -> anyhow::Result<Value> {
        let mut er = HashMap::with_capacity(names.len());
        for (i, n) in names.iter().enumerate() {
            er.insert(n.literal().to_string(), Box::new(values[i].accept(self)?));
        }
        Ok(Value::Record(er))
    }

    fn visit_statement(
        &mut self,
        variable: Option<&'q Token<'q>>,
        body: &'q Ast<'q>,
    ) -> anyhow::Result<Value> {
        // TODO(abhay): fix lifetime issues that come from uncommenting this
        let result = body.accept(self)?;
        if let Some(token) = variable {
            let name = token.literal();
            self.environment.define(name, result.clone());
        }
        Ok(result)
        // unimplemented!()
    }

    fn visit_unary(&mut self, op: &'q Token<'q>, right: &'q Ast<'q>) -> anyhow::Result<Value> {
        match (op.token_type, right.accept(self)?) {
            (TokenType::Minus, Value::Number(n)) => Ok(Value::Number(-n)),
            _ => anyhow::bail!("unaccepted unary call: op {:?} operand {}", op, right),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Eval;
    use crate::lexer::Lexer;
    use crate::parser::Parser;
    use crate::types::Value;
    use std::time::Instant;

    fn expect(exprstr: &str, expected: Value) {
        let lexer = Lexer::new(exprstr);
        let mut start = Instant::now();
        let tokens = lexer.tokenize().unwrap();
        let mut time = start.elapsed();
        println!("Time to lex: {:?}", time);
        let mut parser = Parser::new(tokens);
        start = Instant::now();
        let ast = parser.parse().unwrap();
        time = start.elapsed();
        println!("Time to parse: {:?}", time);

        start = Instant::now();
        let mut e = Eval::new(None);
        let actual = ast.accept(&mut e).unwrap();
        time = start.elapsed();
        println!("Time to eval: {:?}", time);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_add() {
        expect("2 + 3", Value::Number(5.0))
    }

    #[test]
    fn test_query() {
        expect("2 + 3 == 5; ", Value::Bool(true));
        expect("2 + 3 == 5; false", Value::Bool(false));
    }
    #[test]
    fn test_boolops() {
        expect("true or true", Value::Bool(true));
        expect("true or false", Value::Bool(true));
        expect("false and true", Value::Bool(false));
        expect("false and false", Value::Bool(false));
    }
    #[test]
    fn test_relation() {
        expect("2 == 1", Value::Bool(false));
        expect("2 != 1", Value::Bool(true));
        expect("2 == 2", Value::Bool(true));
        expect("2 >= 2", Value::Bool(true));
        expect("2 >= 1", Value::Bool(true));
        expect("2 > 1", Value::Bool(true));
        expect("2 < 1", Value::Bool(false));
        expect("2 <= 1", Value::Bool(false));
        expect("2 <= 2", Value::Bool(true));

        // equality also works on booleans etc
        expect("true == true", Value::Bool(true));
        expect("false == false", Value::Bool(true));
        expect("true == false", Value::Bool(false));
        expect("true != false", Value::Bool(true));
        expect("false != false", Value::Bool(false));

        // and strings
        expect("\"hi\" == \"hi\"", Value::Bool(true));
        expect("\"hi\" != \"hi\"", Value::Bool(false));
        expect("\"hi\" != \"bye\"", Value::Bool(true));
    }
}
