package main

import (
	"log"

	"github.com/Fanamon/flight-processing-service/internal/config"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("port %s, poll %s", cfg.HTTPPort, cfg.OutboxPollInterval)
}
