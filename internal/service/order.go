package service

import (
	"context"
	"fmt"
	"log"
	"order-service/internal/cache"
	"order-service/internal/database"
	"order-service/internal/domain"
)

type Service struct {
	repo  *database.Repository
	cache *cache.Cache
}

func NewService(repo *database.Repository, cache *cache.Cache) *Service {
	return &Service{
		repo:  repo,
		cache: cache,
	}
}

func (s *Service) ProcessOrder(order domain.Order) error {
	ctx := context.Background()

	if err := s.repo.AddOrder(ctx, order); err != nil {
		log.Printf("Error saving to DB: %v", err)
		return err
	}

	s.cache.Set(order.OrderUID, order)
	log.Printf("Order processed successfully: %s", order.OrderUID)
	return nil
}

func (s *Service) GetOrderFromCache(orderUID string) (domain.Order, bool) {
	return s.cache.Get(orderUID)
}

// Получение всех заказов
func (s *Service) GetAllOrdersFromCache() []domain.Order {
	allOrders := s.cache.GetAll()
	orders := make([]domain.Order, 0, len(allOrders))

	for _, order := range allOrders {
		orders = append(orders, order)
	}

	return orders
}

func (s *Service) RestoreCacheFromDB(ctx context.Context) error {
	log.Printf("Restoring cache from database...")

	orders, err := s.repo.GetAllOrders(ctx)
	if err != nil {
		return fmt.Errorf("failed to get orders from DB: %w", err)
	}

	restoredCount := 0
	for _, order := range orders {
		s.cache.Set(order.OrderUID, order)
		restoredCount++
	}

	log.Printf("Successfully restored %d orders to cache", restoredCount)
	return nil
}
func (s *Service) GetOrderFromDB(ctx context.Context, orderUID string) (domain.Order, error) {
	log.Printf("Fetching order from database: %s", orderUID)

	return domain.Order{}, fmt.Errorf("order not found in database")
}

func (s *Service) GetOrder(ctx context.Context, orderUID string) (domain.Order, error) {
	// Сперва проверяем КЭШ
	if order, found := s.cache.Get(orderUID); found {
		log.Printf("Order %s found in cache", orderUID)
		return order, nil
	}

	// Проверяем БД
	order, err := s.GetOrderFromDB(ctx, orderUID)
	if err != nil {
		return domain.Order{}, err
	}

	// Добавляем в КЭШ
	s.cache.Set(orderUID, order)
	log.Printf("Order %s fetched from database and added to cache", orderUID)

	return order, nil
}
