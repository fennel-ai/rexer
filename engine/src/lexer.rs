pub struct Lexer {
    query: Vec<char>,
    current: usize,
    start: usize,
    tokens: Vec<Token>,
}

impl Lexer {
    pub fn new(query: String) -> Self {
        Lexer {
            // TODO: don't remove whitespace here. e.g. "12abc" is no a valid string.
            query: query.chars().filter(|c| !c.is_whitespace()).collect(),
            current: 0,
            start: 0,
            tokens: vec![],
        }
    }

    fn peek(&self) -> Option<char> {
        if self.current >= self.query.len() {
            None
        } else {
            Some(self.query[self.current])
        }
    }

    pub fn tokenize(&mut self) {
        loop {
            let token = self.next();
            if let Token::Eof = token {
                break;
            } else {
                self.tokens.push(token);
            }
        }
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

    fn integer(&mut self) -> Token {
        while let Some(c) = self.peek() {
            if !c.is_numeric() {
                break;
            }
            self.advance();
        }
        let num = self.query[self.start..self.current]
            .iter()
            .collect::<String>();
        Token::Integer(num.parse::<i32>().unwrap())
    }

    pub fn next(&mut self) -> Token {
        if let Some(c) = self.advance() {
            let token = match c {
                '(' => Token::LeftParen,
                ')' => Token::RightParen,
                ',' => Token::Comma,
                '|' => Token::Pipe,
                '+' => Token::Plus,
                ';' => Token::Semicolon,
                '=' => Token::Equal,
                '$' => {
                    self.start += 1;
                    Token::Variable(self.identifier())
                }
                c if c.is_alphabetic() => Token::Identifier(self.identifier()),
                n if n.is_numeric() => self.integer(),
                '[' => Token::ListBegin,
                ']' => Token::ListEnd,
                _ => {
                    panic!("unexpected branch");
                }
            };
            self.start = self.current;
            token
        } else {
            Token::Eof
        }
    }

    fn advance(&mut self) -> Option<char> {
        if self.current >= self.query.len() {
            None
        } else {
            let r = self.query[self.current];
            self.current += 1;
            Some(r)
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
    //STRING,
    Integer(i32),
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
        let mut lexer = Lexer::new(format!("x = 5;  y = [3, $x, 4] | incr(by=$x)"));
        lexer.tokenize();
        let expected = vec![
            Token::Identifier("x".to_string()),
            Token::Equal,
            Token::Integer(5),
            Token::Semicolon,
            Token::Identifier("y".to_string()),
            Token::Equal,
            Token::ListBegin,
            Token::Integer(3),
            Token::Comma,
            Token::Variable("x".to_string()),
            Token::Comma,
            Token::Integer(4),
            Token::ListEnd,
            Token::Pipe,
            Token::Identifier("incr".to_string()),
            Token::LeftParen,
            Token::Identifier("by".to_string()),
            Token::Equal,
            Token::Variable("x".to_string()),
            Token::RightParen,
        ];
        assert_eq!(expected, lexer.tokens);
    }
}
