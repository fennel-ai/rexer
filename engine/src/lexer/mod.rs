#[cfg(test)]
mod tests;

pub struct Lexer<'a> {
    query: &'a [char],
    current: usize,
    start: usize,
    line: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(query: &'a [char]) -> Self {
        Lexer {
            query: query,
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

    pub fn tokenize(mut self) -> anyhow::Result<Vec<Token<'a>>> {
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
        });
        return Ok(tokens);
    }

    fn identifier(&mut self) {
        while let Some(c) = self.peek() {
            if !c.is_alphabetic() {
                break;
            }
            self.advance();
        }
    }

    fn string(&mut self) -> anyhow::Result<()> {
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
        Ok(())
    }

    fn parse_digits(&mut self) {
        while let Some(c) = self.peek() {
            if !c.is_numeric() {
                break;
            }
            self.advance();
        }
    }

    fn number(&mut self) -> anyhow::Result<()> {
        self.parse_digits();
        // Look for a decimal point
        if let Some('.') = self.peek() {
            self.advance();
            if let Some(c) = self.peek() {
                match c {
                    n if n.is_numeric() => {
                        self.parse_digits();
                    }
                    _ => {
                        anyhow::bail!("expected digits after '.'");
                    }
                }
            }
        }
        Ok(())
    }

    fn new_token(&self, token_type: TokenType) -> Token<'a> {
        Token {
            token_type: token_type,
            lexeme: &self.query[self.start..self.current],
        }
    }

    pub fn next(&mut self) -> anyhow::Result<Option<Token>> {
        if let Some(c) = self.advance() {
            match c {
                '(' => Ok(Some(self.new_token(TokenType::LeftParen))),
                ')' => Ok(Some(self.new_token(TokenType::RightParen))),
                '[' => Ok(Some(self.new_token(TokenType::ListBegin))),
                ']' => Ok(Some(self.new_token(TokenType::ListEnd))),
                '{' => Ok(Some(self.new_token(TokenType::RecordBegin))),
                '}' => Ok(Some(self.new_token(TokenType::RecordEnd))),
                ',' => Ok(Some(self.new_token(TokenType::Comma))),
                '.' => Ok(Some(self.new_token(TokenType::Dot))),
                '|' => Ok(Some(self.new_token(TokenType::Pipe))),
                '+' => Ok(Some(self.new_token(TokenType::Plus))),
                '-' => Ok(Some(self.new_token(TokenType::Minus))),
                '*' => Ok(Some(self.new_token(TokenType::Star))),
                // TODO(abhay): Handle comments.
                '/' => Ok(Some(self.new_token(TokenType::Slash))),
                ';' => Ok(Some(self.new_token(TokenType::Semicolon))),
                '=' => {
                    if let Some('=') = self.peek() {
                        // consume the '='
                        self.advance().unwrap();
                        Ok(Some(self.new_token(TokenType::EqualEqual)))
                    } else {
                        Ok(Some(self.new_token(TokenType::Equal)))
                    }
                }
                '>' => {
                    if let Some('=') = self.peek() {
                        // consume the '='
                        self.advance().unwrap();
                        Ok(Some(self.new_token(TokenType::GreaterEqual)))
                    } else {
                        Ok(Some(self.new_token(TokenType::Greater)))
                    }
                }
                '<' => {
                    if let Some('=') = self.peek() {
                        // consume the '='
                        self.advance().unwrap();
                        Ok(Some(self.new_token(TokenType::LesserEqual)))
                    } else {
                        Ok(Some(self.new_token(TokenType::Lesser)))
                    }
                }
                '!' => {
                    if let Some('=') = self.peek() {
                        // consume the '='
                        self.advance().unwrap();
                        Ok(Some(self.new_token(TokenType::BangEqual)))
                    } else {
                        Ok(Some(self.new_token(TokenType::Bang)))
                    }
                }
                '"' => {
                    self.string()?;
                    Ok(Some(self.new_token(TokenType::String)))
                }
                c if c.is_alphabetic() => {
                    self.identifier();
                    // TODO: clean this up.
                    match self.query[self.start..self.current]
                        .iter()
                        .collect::<String>()
                        .as_ref()
                    {
                        "true" | "false" => Ok(Some(self.new_token(TokenType::Bool))),
                        "or" => Ok(Some(self.new_token(TokenType::Or))),
                        "and" => Ok(Some(self.new_token(TokenType::And))),
                        _ => Ok(Some(self.new_token(TokenType::Identifier))),
                    }
                }
                n if n.is_numeric() => {
                    self.number()?;
                    Ok(Some(self.new_token(TokenType::Number)))
                }
                '$' => {
                    match self.peek() {
                        Some(c) if c.is_alphabetic() => {
                            self.identifier();
                        }
                        _ => anyhow::bail!("identifier names should start with alphabetic chars"),
                    }
                    Ok(Some(self.new_token(TokenType::Variable)))
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

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Token<'a> {
    pub token_type: TokenType,
    pub lexeme: &'a [char],
    // TODO: add line and pos information.
}

impl<'a> Token<'a> {
    // todo(abhay): return reference.
    pub fn literal(&self) -> String {
        match self.token_type {
            TokenType::String => self.lexeme[1..self.lexeme.len() - 1].to_string(),
            TokenType::Variable => self.lexeme[1..].to_string(),
            _ => self.lexeme.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
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
    Semicolon,
    Equal,

    // Arithmetic operaotrs
    Plus,
    Minus,
    Star,
    Slash,

    // Unary bool op.
    Bang,

    // Relational operations
    Greater,
    Lesser,
    GreaterEqual,
    LesserEqual,
    EqualEqual,
    BangEqual,

    // Keywords
    Or,
    And,

    // All rest
    Identifier,
    Variable,
    String,
    Number,
    Bool,
    Eof,
}
