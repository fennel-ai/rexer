pub struct Lexer {
    query: Vec<char>,
    current: usize,
    start: usize,
}

impl Lexer {
    pub fn new(query: String) -> Self {
        Lexer {
            query: query.chars().collect(),
            current: 0,
            start: 0,
        }
    }

    fn peek(&self) -> Option<&char> {
        self.query.get(self.current)
    }

    fn peek_next(&self) -> Option<&char> {
        self.query.get(self.current + 1)
    }

    fn advance(&mut self) -> Option<&char> {
        let r = self.query.get(self.current);
        self.current += 1;
        r
    }

    fn done(&self) -> bool {
        self.current >= self.query.len()
    }

    pub fn tokenize(mut self) -> anyhow::Result<Vec<Token>> {
        let mut tokens = vec![];
        while !self.done() {
            self.start = self.current;
            // TODO: consume error but continue lexing.
            if let Some(token) = self.next()? {
                tokens.push(token);
            }
        }
        tokens.push(Token::Eof);
        return Ok(tokens);
    }

    fn identifier(&mut self) -> String {
        while let Some(c) = self.peek() {
            if !c.is_alphabetic() {
                break;
            }
            self.advance();
        }
        self.query[self.start..self.current].iter().collect()
    }

    fn string(&mut self) -> anyhow::Result<String> {
        while let Some(c) = self.peek() {
            if *c == '"' {
                break;
            }
            self.advance();
        }
        if self.done() {
            anyhow::bail!("string without trailing \"");
        }
        // advance over the closing '"'.
        self.advance();
        let r = self.query[self.start + 1..self.current - 1]
            .iter()
            .collect();
        Ok(r)
    }

    fn parse_digits(&mut self) {
        while let Some(c) = self.peek() {
            if !c.is_numeric() {
                break;
            }
            self.advance();
        }
    }

    fn number(&mut self) -> f64 {
        self.parse_digits();
        // Look for a decimal point
        if let Some('.') = self.peek() {
            if let Some(c) = self.peek_next() {
                match c {
                    n if n.is_numeric() => {
                        self.advance();
                        self.parse_digits();
                    }
                    _ => {}
                }
            }
        }

        let num = self.query[self.start..self.current]
            .iter()
            .collect::<String>();
        num.parse::<f64>().unwrap()
    }

    pub fn next(&mut self) -> anyhow::Result<Option<Token>> {
        if let Some(c) = self.advance() {
            match c {
                '(' => {
                    return Ok(Some(Token::LeftParen));
                }
                ')' => {
                    return Ok(Some(Token::RightParen));
                }
                ',' => {
                    return Ok(Some(Token::Comma));
                }
                '|' => {
                    return Ok(Some(Token::Pipe));
                }
                '+' => {
                    return Ok(Some(Token::Plus));
                }
                ';' => {
                    return Ok(Some(Token::Semicolon));
                }
                '=' => {
                    return Ok(Some(Token::Equal));
                }
                '$' => {
                    self.start += 1;
                    return Ok(Some(Token::Variable(self.identifier())));
                }
                '"' => {
                    return Ok(Some(Token::String(self.string()?)));
                }
                c if c.is_alphabetic() => {
                    return Ok(Some(Token::Identifier(self.identifier())));
                }
                n if n.is_numeric() => {
                    return Ok(Some(Token::Number(self.number())));
                }
                '[' => {
                    return Ok(Some(Token::ListBegin));
                }
                ']' => {
                    return Ok(Some(Token::ListEnd));
                }
                ' ' => return Ok(None),
                '\t' => return Ok(None),
                '\r' => return Ok(None),
                // TODO: Increment a line number for better debugging.
                '\n' => return Ok(None),
                _ => {
                    anyhow::bail!("unexpected character: {:?}", *c);
                }
            }
        } else {
            return Ok(None);
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum Token {
    // Operator calls
    LeftParen,
    RightParen,
    Comma,
    Pipe,
    // Characters
    Plus,
    Semicolon,
    // For assignment
    Equal,
    Identifier(String),
    Variable(String),
    // Literals.
    String(String),
    Number(f64),
    // Lists
    ListBegin,
    ListEnd,
    Eof,
}

impl Token {
    pub fn lexene(&self) -> String {
        match self {
            Token::LeftParen => "(".to_string(),
            Token::RightParen => ")".to_string(),
            Token::Comma => ",".to_string(),
            Token::Pipe => "|".to_string(),
            Token::Plus => "+".to_string(),
            Token::Semicolon => ";".to_string(),
            Token::Equal => "=".to_string(),
            Token::Identifier(ref id) => id.clone(),
            Token::Variable(ref id) => format!("${}", id),
            Token::String(ref str) => str.clone(),
            Token::Number(n) => format!("{}", n),
            Token::ListBegin => "[".to_string(),
            Token::ListEnd => "]".to_string(),
            Token::Eof => {
                unreachable!("lexene called on EOF")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Lexer;
    use super::Token;

    #[test]
    fn lex_paren() {
        let lexer = Lexer::new(format!(
            "x = 05.13;  z = \"foo\"; y = [3, $x, 4] | incr(by=$x)"
        ));
        let actual = lexer.tokenize().unwrap();
        let expected = vec![
            Token::Identifier("x".to_string()),
            Token::Equal,
            Token::Number(5.13 as f64),
            Token::Semicolon,
            Token::Identifier("z".to_string()),
            Token::Equal,
            Token::String("foo".to_string()),
            Token::Semicolon,
            Token::Identifier("y".to_string()),
            Token::Equal,
            Token::ListBegin,
            Token::Number(3 as f64),
            Token::Comma,
            Token::Variable("x".to_string()),
            Token::Comma,
            Token::Number(4 as f64),
            Token::ListEnd,
            Token::Pipe,
            Token::Identifier("incr".to_string()),
            Token::LeftParen,
            Token::Identifier("by".to_string()),
            Token::Equal,
            Token::Variable("x".to_string()),
            Token::RightParen,
            Token::Eof,
        ];
        assert_eq!(expected, actual);
    }
}
