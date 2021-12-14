package ast

type TokenType uint8

const (
	LPAREN TokenType = 1
	RPAREN           = 2
)

type Token struct {
	query string
	Type  TokenType
	start uint32
	end   uint32
}

func (t Token) literal() string {
	return "TODO"
}
