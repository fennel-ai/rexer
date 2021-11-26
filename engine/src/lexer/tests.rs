use super::Lexer;
use super::{Token, TokenType, TokenValue};

fn new_token(token_type: TokenType, value: Option<TokenValue>, lexeme: String) -> Token {
    Token {
        token_type: token_type,
        literal: value,
        lexeme: lexeme,
    }
}
#[test]
fn lex_paren() {
    let lexer = Lexer::new(format!(
        "x = 05.13;  z = \"foo\"; y = [3, x, 4] | incr(by=x)"
    ));
    let actual = lexer.tokenize().unwrap();
    let expected: Vec<Token> = vec![
        new_token(TokenType::Identifier, Some(TokenValue::String("x".to_string())), "x".to_string())
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
