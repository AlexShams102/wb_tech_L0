package main

import (
	"context"
	"log"
	"net/http"
	"order-service/internal/cache"
	"order-service/internal/database"
	"order-service/internal/handler"
	"order-service/internal/message"
	"order-service/internal/service"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Инициализируем БД
	dbConnStr := "postgres://order_user:111111@localhost:5432/order_service?sslmode=disable"
	repo, err := database.New(ctx, dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer repo.Close()

	// 2. Инициализируем кэш
	cache := cache.New()

	// 3. Инициализируем сервис
	svc := service.NewService(repo, cache)

	// 4. Восстанавливаем КЭШ из БД
	log.Printf("Restoring cache from database...")
	if err := svc.RestoreCacheFromDB(ctx); err != nil {
		log.Printf("Warning: could not restore cache from DB: %v", err)
	} else {
		log.Printf("Cache restored successfully")
	}

	// 5. Инициализируем Kafka consumer
	kafkaConsumer := message.NewKafkaConsumer(
		[]string{"localhost:9092"},
		"orders",
		"order-service-group",
		svc,
	)
	defer kafkaConsumer.Close()

	// 6. Запускаем Kafka consumer в горутине
	go kafkaConsumer.Start(ctx)

	// 7. Инициализируем HTTP handler
	handler := handler.New(svc)
	httpServer := &http.Server{
		Addr:    ":8081",
		Handler: handler.InitRoutes(),
	}

	// 8. Запускаем HTTP сервер
	go func() {
		log.Printf("Starting HTTP server on :8081")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	log.Printf("Service started successfully! Waiting for messages...")

	// 9. Обработка сигналов для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Printf("Shutting down...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Останавливаем HTTP сервер
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// Останавливаем Kafka consumer
	kafkaConsumer.Close()
	cancel()

	log.Printf("Service stopped gracefully")
}
