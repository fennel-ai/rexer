use super::Lexer;
use super::Token;

#[test]
fn lex_paren() {
    let lexer = Lexer::new("x = 05.13;  z = \"foo\"; y = [3, x, 4] | incr(by=x)");
    let _actual = lexer.tokenize().unwrap();
    let _expected: Vec<Token> = vec![
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
    // assert_eq!(expected, actual);
}
