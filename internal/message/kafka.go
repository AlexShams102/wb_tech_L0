package message

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"order-service/internal/domain"
	"order-service/internal/service"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader  *kafka.Reader
	service *service.Service
}

func NewKafkaConsumer(brokers []string, topic string, groupID string, svc *service.Service) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	log.Printf("Kafka Consumer configured:")
	log.Printf("  Brokers: %v", brokers)
	log.Printf("  Topic: %s", topic)
	log.Printf("  GroupID: %s", groupID)

	return &Consumer{
		reader:  reader,
		service: svc,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	log.Printf("Starting Kafka consumer...")

	log.Printf("Kafka consumer started successfully. Waiting for messages...")

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping Kafka consumer")
			c.reader.Close()
			return
		default:
			c.readMessage(ctx)
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Consumer) readMessage(ctx context.Context) {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	msg, err := c.reader.ReadMessage(readCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Printf("No messages received in 10 seconds (waiting...)")
		} else {
			log.Printf("Error reading message: %v", err)
		}
		return
	}

	log.Printf("✅ Received new message!")
	log.Printf("   Partition: %d, Offset: %d", msg.Partition, msg.Offset)
	log.Printf("   Message size: %d bytes", len(msg.Value))

	c.processMessage(msg.Value)
}

func (c *Consumer) processMessage(data []byte) {
	// Проверяем JSON
	if !isValidJSON(data) {
		log.Printf("❌ Invalid JSON message received: %s", string(data[:200]))
		return
	}

	var order domain.Order
	if err := json.Unmarshal(data, &order); err != nil {
		log.Printf("❌ Error parsing JSON: %v", err)
		log.Printf("Raw message (first 200 chars): %s", string(data[:200]))
		return
	}

	if err := validateOrder(order); err != nil {
		log.Printf("❌ Invalid order data: %v", err)
		log.Printf("Order UID: %s", order.OrderUID)
		return
	}

	if err := c.service.ProcessOrder(order); err != nil {
		log.Printf("❌ Error processing order: %v", err)
		return
	}

	log.Printf("✅ Successfully processed order: %s", order.OrderUID)
}

func isValidJSON(data []byte) bool {
	return json.Valid(data)
}

func validateOrder(order domain.Order) error {
	if order.OrderUID == "" {
		return errors.New("order_uid is required")
	}
	if order.TrackNumber == "" {
		return errors.New("track_number is required")
	}
	if order.Payment.Transaction == "" {
		return errors.New("payment.transaction is required")
	}
	if len(order.Items) == 0 {
		return errors.New("at least one item is required")
	}
	return nil
}

func (c *Consumer) Close() error {
	log.Printf("Closing Kafka consumer")
	return c.reader.Close()
}
