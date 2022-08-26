package main

import (
	"fmt"
	"log"
)

func main() {
	s, err := NewServer()
	if err != nil {
		log.Fatalf("Error creating the server: %s", err)
	}
	if err := s.Run(fmt.Sprintf(":%s", s.args.AppPort)); err != nil {
		log.Fatalf("Error running the server: %s", err)
	}
}
