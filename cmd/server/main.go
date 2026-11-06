package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Fanamon/flight-processing-service/internal/config"
	"github.com/Fanamon/flight-processing-service/internal/handlers"
	"github.com/Fanamon/flight-processing-service/internal/kafka"
	"github.com/Fanamon/flight-processing-service/internal/repository"
	"github.com/Fanamon/flight-processing-service/internal/service"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
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

	db, err := sql.Open("pgx", cfg.DBDSN)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	m, err := migrate.New("file://migrations", cfg.DBDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatal(err)
	}

	metaRepo := repository.NewMetaRepository(db)
	flightRepo := repository.NewFlightRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)
	flightService := service.NewFlightService(db, metaRepo, outboxRepo, flightRepo, cfg.KafkaTopic)
	flightHandler := handlers.NewFlightHandler(flightService)

	brokers := strings.Split(cfg.KafkaBrokers, ",")
	syncProducer, err := kafka.NewSyncProducer(brokers, cfg.KafkaTopic)
	if err != nil {
		log.Fatal(err)
	}
	defer syncProducer.Close()

	consumerGroup, err := kafka.NewConsumerGroup(brokers, cfg.KafkaGroupID, cfg.KafkaTopic, flightService)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerGroup.Close()

	outboxSender := kafka.NewOutboxSender(outboxRepo, syncProducer, cfg.OutboxPollInterval, cfg.OutboxBatchSize,
		cfg.OutboxMaxRetries, cfg.OutboxRetentionDays)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := consumerGroup.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("consumer: %v", err)
		}
	}()

	go func() {
		if err := outboxSender.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("outbox sender: %v", err)
		}
	}()

	mux := flightHandler.Routes()
	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server: %v", err)
		}
	}()

	log.Printf("listening on :%s", cfg.HTTPPort)
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
}
