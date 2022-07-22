package main

import (
	"log"
)

func main() {
	r := setupRouter()
	// Listen and Server in 0.0.0.0:8080
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Error running the server: %s", err)
	}
}
