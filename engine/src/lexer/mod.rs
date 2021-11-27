#[cfg(test)]
mod tests;

pub struct Lexer {
    query: Vec<char>,
    current: usize,
    start: usize,
    line: usize,
}

impl Lexer {
    pub fn new(query: String) -> Self {
        Lexer {
            query: query.chars().collect(),
            current: 0,
            start: 0,
            line: 0,
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
                '(' => Ok(Some(self.new_token(TokenType::LeftParen, None))),
                ')' => Ok(Some(self.new_token(TokenType::RightParen, None))),
                '[' => Ok(Some(self.new_token(TokenType::ListBegin, None))),
                ']' => Ok(Some(self.new_token(TokenType::ListEnd, None))),
                '{' => Ok(Some(self.new_token(TokenType::RecordBegin, None))),
                '}' => Ok(Some(self.new_token(TokenType::RecordEnd, None))),
                ',' => Ok(Some(self.new_token(TokenType::Comma, None))),
                '.' => Ok(Some(self.new_token(TokenType::Dot, None))),
                '|' => Ok(Some(self.new_token(TokenType::Pipe, None))),
                '+' => Ok(Some(self.new_token(TokenType::Plus, None))),
                '-' => Ok(Some(self.new_token(TokenType::Minus, None))),
                '*' => Ok(Some(self.new_token(TokenType::Star, None))),
                // TODO(abhay): Handle comments.
                '/' => Ok(Some(self.new_token(TokenType::Slash, None))),
                ';' => Ok(Some(self.new_token(TokenType::Semicolon, None))),
                '=' => {
                    if let Some('=') = self.peek() {
                        // consume the '='
                        self.advance().unwrap();
                        Ok(Some(self.new_token(TokenType::EqualEqual, None)))
                    } else {
                        Ok(Some(self.new_token(TokenType::Equal, None)))
                    }
                }
                '>' => {
                    if let Some('=') = self.peek() {
                        // consume the '='
                        self.advance().unwrap();
                        Ok(Some(self.new_token(TokenType::GreaterEqual, None)))
                    } else {
                        Ok(Some(self.new_token(TokenType::Greater, None)))
                    }
                }
                '<' => {
                    if let Some('=') = self.peek() {
                        // consume the '='
                        self.advance().unwrap();
                        Ok(Some(self.new_token(TokenType::LesserEqual, None)))
                    } else {
                        Ok(Some(self.new_token(TokenType::Lesser, None)))
                    }
                }
                '!' => {
                    if let Some('=') = self.peek() {
                        // consume the '='
                        self.advance().unwrap();
                        Ok(Some(self.new_token(TokenType::BangEqual, None)))
                    } else {
                        Ok(Some(self.new_token(TokenType::Bang, None)))
                    }
                }
                '"' => {
                    let s = self.string()?;
                    Ok(Some(
                        self.new_token(TokenType::String, Some(TokenValue::String(s))),
                    ))
                }
                c if c.is_alphabetic() => {
                    let s = self.identifier();
                    let token_type = match s.as_ref() {
                        "true" => TokenType::True,
                        "false" => TokenType::False,
                        "or" => TokenType::Or,
                        "and" => TokenType::And,
                        _ => TokenType::Identifier,
                    };
                    Ok(Some(
                        self.new_token(token_type, Some(TokenValue::String(s))),
                    ))
                }
                n if n.is_numeric() => {
                    let n = self.number();
                    Ok(Some(
                        self.new_token(TokenType::Number, Some(TokenValue::Double(n))),
                    ))
                }
                ' ' => Ok(None),
                '\t' => Ok(None),
                '\r' => Ok(None),
                // TODO: Increment a line number for better debugging.
                '\n' => {
                    self.line += 1;
                    Ok(None)
                }
                _ => anyhow::bail!("unexpected character: {:?}", *c),
            }
        } else {
            Ok(None)
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
    // Characters
    LeftParen,
    RightParen,
    ListBegin,
    ListEnd,
    RecordBegin,
    RecordEnd,
    Comma,
    Dot,
    Pipe,
    Plus,
    Minus,
    Star,
    Slash,
    Semicolon,
    Equal,
    Greater,
    Lesser,
    Bang,

    // Double character binary operations
    GreaterEqual,
    LesserEqual,
    EqualEqual,
    BangEqual,

    // Keywords
    True,
    False,
    Or,
    And,

    // All rest
    Identifier,
    String,
    Number,
    Eof,
}
