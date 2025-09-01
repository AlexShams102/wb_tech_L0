package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"order-service/internal/domain"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Генерируем уникальный ID
	uniqueID := fmt.Sprintf("test-%d", time.Now().UnixNano())

	// Создаем тестовый заказ
	testOrder := domain.Order{
		OrderUID:          uniqueID,
		TrackNumber:       "WBILMTESTTRACK",
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        "test",
		DeliveryService:   "meest",
		Shardkey:          "9",
		SmID:              99,
		DateCreated:       time.Now(),
		OofShard:          "1",
		Delivery: domain.Delivery{
			Name:    "Test Testov",
			Phone:   "+9720000000",
			Zip:     "2639809",
			City:    "Kiryat Mozkin",
			Address: "Ploshad Mira 15",
			Region:  "Kraiot",
			Email:   "test@gmail.com",
		},
		Payment: domain.Payment{
			Transaction:  uniqueID,
			RequestID:    "",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       1817,
			PaymentDt:    1637907727,
			Bank:         "alpha",
			DeliveryCost: 1500,
			GoodsTotal:   317,
			CustomFee:    0,
		},
		Items: []domain.Item{
			{
				ChrtID:      9934930,
				TrackNumber: "WBILMTESTTRACK",
				Price:       453,
				Rid:         "ab4219087a764ae0btest",
				Name:        "Mascaras",
				Sale:        30,
				Size:        "0",
				TotalPrice:  317,
				NmID:        2389212,
				Brand:       "Vivienne Sabo",
				Status:      202,
			},
		},
	}

	// JSON
	jsonData, err := json.Marshal(testOrder)
	if err != nil {
		log.Fatalf("Error marshaling JSON: %v", err)
	}

	// Создаем Kafka producer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "orders",
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Отправляем сообщение
	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: jsonData,
		},
	)
	if err != nil {
		log.Fatalf("Error sending message: %v", err)
	}

	log.Printf("✅ Message sent successfully!")
	log.Printf("Order UID: %s", testOrder.OrderUID)
	log.Printf("Message size: %d bytes", len(jsonData))
}
