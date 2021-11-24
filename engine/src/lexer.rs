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
        tokens.push(Token {
            token_type: TokenType::Eof,
            lexeme: "".to_string(),
            literal: None,
        });
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

    fn new_token(&self, token_type: TokenType, value: Option<TokenValue>) -> Token {
        Token {
            token_type: token_type,
            literal: value,
            lexeme: self.query[self.start..self.current].iter().collect(),
        }
    }

    pub fn next(&mut self) -> anyhow::Result<Option<Token>> {
        if let Some(c) = self.advance() {
            match c {
                '(' => {
                    return Ok(Some(self.new_token(TokenType::LeftParen, None)));
                }
                ')' => {
                    return Ok(Some(self.new_token(TokenType::RightParen, None)));
                }
                ',' => {
                    return Ok(Some(self.new_token(TokenType::Comma, None)));
                }
                '|' => {
                    return Ok(Some(self.new_token(TokenType::Pipe, None)));
                }
                '+' => {
                    return Ok(Some(self.new_token(TokenType::Plus, None)));
                }
                '-' => {
                    return Ok(Some(self.new_token(TokenType::Minus, None)));
                }
                '*' => {
                    return Ok(Some(self.new_token(TokenType::Star, None)));
                }
                '/' => {
                    // TODO(abhay): Handle comments.
                    return Ok(Some(self.new_token(TokenType::Slash, None)));
                }
                ';' => {
                    return Ok(Some(self.new_token(TokenType::Semicolon, None)));
                }
                '=' => {
                    return Ok(Some(self.new_token(TokenType::Equal, None)));
                }
                '[' => {
                    return Ok(Some(self.new_token(TokenType::ListBegin, None)));
                }
                ']' => {
                    return Ok(Some(self.new_token(TokenType::ListEnd, None)));
                }
                '"' => {
                    let s = self.string()?;
                    return Ok(Some(
                        self.new_token(TokenType::String, Some(TokenValue::String(s))),
                    ));
                }
                c if c.is_alphabetic() => {
                    let s = self.identifier();
                    return Ok(Some(
                        self.new_token(TokenType::Identifier, Some(TokenValue::String(s))),
                    ));
                }
                n if n.is_numeric() => {
                    let n = self.number();
                    return Ok(Some(
                        self.new_token(TokenType::Number, Some(TokenValue::Double(n))),
                    ));
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

#[derive(Debug, PartialEq)]
pub enum TokenValue {
    String(String),
    Double(f64),
}

#[derive(Debug, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub literal: Option<TokenValue>,
    pub lexeme: String,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TokenType {
    // Operator calls
    LeftParen,
    RightParen,
    Comma,
    Pipe,
    // Characters
    Plus,
    Minus,
    Star,
    Slash,
    Semicolon,
    // For assignment
    Equal,
    Identifier,
    // Literals.
    String,
    Number,
    // Lists
    ListBegin,
    ListEnd,
    Eof,
}

#[cfg(test)]
mod tests {
    use super::Lexer;
    use super::Token;

    #[test]
    fn lex_paren() {
        let lexer = Lexer::new(format!(
            "x = 05.13;  z = \"foo\"; y = [3, x, 4] | incr(by=x)"
        ));
        let actual = lexer.tokenize().unwrap();
        let expected: Vec<Token> = vec![
            // TokenType::Identifier("x".to_string()),
            // TokenType::Equal,
            // TokenType::Number(5.13 as f64),
            // TokenType::Semicolon,
            // TokenType::Identifier("z".to_string()),
            // TokenType::Equal,
            // TokenType::String("foo".to_string()),
            // TokenType::Semicolon,
            // TokenType::Identifier("y".to_string()),
            // TokenType::Equal,
            // TokenType::ListBegin,
            // TokenType::Number(3 as f64),
            // TokenType::Comma,
            // TokenType::Identifier("x".to_string()),
            // TokenType::Comma,
            // TokenType::Number(4 as f64),
            // TokenType::ListEnd,
            // TokenType::Pipe,
            // TokenType::Identifier("incr".to_string()),
            // TokenType::LeftParen,
            // TokenType::Identifier("by".to_string()),
            // TokenType::Equal,
            // TokenType::Identifier("x".to_string()),
            // TokenType::RightParen,
            // TokenType::Eof,
        ];
        assert_eq!(expected, actual);
    }
}
