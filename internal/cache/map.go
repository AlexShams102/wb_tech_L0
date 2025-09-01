package cache

import (
	"order-service/internal/domain"
	"sync"
)

type Cache struct {
	mu   sync.RWMutex
	data map[string]domain.Order
}

func New() *Cache {
	return &Cache{
		data: make(map[string]domain.Order),
	}
}

func (c *Cache) Set(uid string, order domain.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[uid] = order
}

func (c *Cache) Get(uid string) (domain.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	order, exists := c.data[uid]
	return order, exists
}

func (c *Cache) Delete(uid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, uid)
}

func (c *Cache) GetAll() map[string]domain.Order {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]domain.Order, len(c.data))
	for k, v := range c.data {
		result[k] = v
	}
	return result
}

func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}
