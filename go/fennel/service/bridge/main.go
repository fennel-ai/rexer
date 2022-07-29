package main

import (
	"log"
)

func main() {
	s, err := NewServer()
	if err != nil {
		log.Fatalf("Error creating the server: %s", err)
	}
	// Listen and Server in 0.0.0.0:8080
	if err := s.Run(":8080"); err != nil {
		log.Fatalf("Error running the server: %s", err)
	}
}
