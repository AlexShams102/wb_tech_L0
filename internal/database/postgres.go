package database

import (
	"context"
	"fmt"
	"log"
	"order-service/internal/domain"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, connString string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Проверяем подключение
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	log.Printf("✅ Database connection pool created successfully")
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	r.pool.Close()
	log.Printf("✅ Database connection pool closed")
}

func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

func (r *Repository) AddOrder(ctx context.Context, order domain.Order) error {
	log.Printf("🟡 Starting to save order: %s", order.OrderUID)

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		log.Printf("🔴 Failed to begin transaction: %v", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Вставляем в orders
	log.Printf("🟡 Inserting into orders: %s", order.OrderUID)
	_, err = tx.Exec(ctx, `
		INSERT INTO orders (
			order_uid, track_number, entry, locale, internal_signature, 
			customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, order.OrderUID, order.TrackNumber, order.Entry, order.Locale,
		order.InternalSignature, order.CustomerID, order.DeliveryService,
		order.Shardkey, order.SmID, order.DateCreated, order.OofShard)

	if err != nil {
		log.Printf("🔴 Error inserting into orders: %v", err)
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("🔴 PostgreSQL Error: %s", pgErr.Message)
			log.Printf("🔴 Detail: %s", pgErr.Detail)
			log.Printf("🔴 Where: %s", pgErr.Where)
			log.Printf("🔴 SQLState: %s", pgErr.SQLState())
		}
		return fmt.Errorf("failed to insert into orders: %w", err)
	}
	log.Printf("✅ Orders inserted successfully")

	// 2. Вставляем delivery
	log.Printf("🟡 Inserting into deliveries: %s", order.OrderUID)
	_, err = tx.Exec(ctx, `
		INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, order.OrderUID, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)

	if err != nil {
		log.Printf("🔴 ERROR inserting into deliveries: %v", err)
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("🔴 PostgreSQL Error: %s", pgErr.Message)
			log.Printf("🔴 Detail: %s", pgErr.Detail)
			log.Printf("🔴 Where: %s", pgErr.Where)
			log.Printf("🔴 SQLState: %s", pgErr.SQLState())
		}
		return fmt.Errorf("failed to insert into deliveries: %w", err)
	}
	log.Printf("✅ Deliveries inserted successfully")

	// 3. Вставляем payment
	log.Printf("🟡 Inserting into payments: %s", order.OrderUID)
	_, err = tx.Exec(ctx, `
		INSERT INTO payments (transaction, request_id, currency, provider, amount, 
							payment_dt, bank, delivery_cost, goods_total, custom_fee)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, order.Payment.Transaction, order.Payment.RequestID, order.Payment.Currency,
		order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDt,
		order.Payment.Bank, order.Payment.DeliveryCost, order.Payment.GoodsTotal,
		order.Payment.CustomFee)

	if err != nil {
		log.Printf("🔴 ERROR inserting into payments: %v", err)
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("🔴 PostgreSQL Error: %s", pgErr.Message)
			log.Printf("🔴 Detail: %s", pgErr.Detail)
			log.Printf("🔴 Where: %s", pgErr.Where)
			log.Printf("🔴 SQLState: %s", pgErr.SQLState())
		}
		return fmt.Errorf("failed to insert into payments: %w", err)
	}
	log.Printf("✅ Payments inserted successfully")

	// 4. Вставляем items
	log.Printf("🟡 Inserting %d items", len(order.Items))
	for i, item := range order.Items {
		_, err = tx.Exec(ctx, `
			INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, 
							sale, size, total_price, nm_id, brand, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`, order.OrderUID, item.ChrtID, item.TrackNumber, item.Price, item.Rid,
			item.Name, item.Sale, item.Size, item.TotalPrice, item.NmID,
			item.Brand, item.Status)

		if err != nil {
			log.Printf("🔴 ERROR inserting item %d: %v", i+1, err)
			if pgErr, ok := err.(*pgconn.PgError); ok {
				log.Printf("🔴 PostgreSQL Error: %s", pgErr.Message)
				log.Printf("🔴 Detail: %s", pgErr.Detail)
				log.Printf("🔴 Where: %s", pgErr.Where)
				log.Printf("🔴 SQLState: %s", pgErr.SQLState())
			}
			return fmt.Errorf("failed to insert into items: %w", err)
		}
		log.Printf("✅ Item %d inserted successfully", i+1)
	}

	// Коммитим транзакцию
	if err := tx.Commit(ctx); err != nil {
		log.Printf("🔴 Transaction commit error: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("🎉 Order %s saved successfully to all tables!", order.OrderUID)
	return nil
}

func (r *Repository) GetAllOrders(ctx context.Context) ([]domain.Order, error) {
	log.Printf("Getting all orders from database for cache restoration...")

	query := `
        SELECT 
            o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature,
            o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
            d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
            p.request_id, p.currency, p.provider, p.amount, p.payment_dt, p.bank, 
            p.delivery_cost, p.goods_total, p.custom_fee
        FROM orders o
        LEFT JOIN deliveries d ON o.order_uid = d.order_uid
        LEFT JOIN payments p ON o.order_uid = p.transaction
        ORDER BY o.date_created DESC
    `

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query orders: %w", err)
	}
	defer rows.Close()

	var orders []domain.Order
	for rows.Next() {
		var order domain.Order
		var delivery domain.Delivery
		var payment domain.Payment

		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSignature,
			&order.CustomerID, &order.DeliveryService, &order.Shardkey, &order.SmID, &order.DateCreated, &order.OofShard,
			&delivery.Name, &delivery.Phone, &delivery.Zip, &delivery.City, &delivery.Address, &delivery.Region, &delivery.Email,
			&payment.RequestID, &payment.Currency, &payment.Provider, &payment.Amount, &payment.PaymentDt, &payment.Bank,
			&payment.DeliveryCost, &payment.GoodsTotal, &payment.CustomFee,
		)

		if err != nil {
			log.Printf("Error scanning order: %v", err)
			continue
		}

		order.Delivery = delivery
		order.Payment = payment
		order.Payment.Transaction = order.OrderUID

		items, err := r.getOrderItems(ctx, order.OrderUID)
		if err != nil {
			log.Printf("Error getting items for order %s: %v", order.OrderUID, err)
		} else {
			order.Items = items
		}

		orders = append(orders, order)
	}

	log.Printf("Successfully retrieved %d orders from database", len(orders))
	return orders, nil
}

func (r *Repository) getOrderItems(ctx context.Context, orderUID string) ([]domain.Item, error) {
	query := `
        SELECT chrt_id, track_number, price, rid, name, sale, size, 
               total_price, nm_id, brand, status
        FROM items 
        WHERE order_uid = $1
    `

	rows, err := r.pool.Query(ctx, query, orderUID)
	if err != nil {
		return nil, fmt.Errorf("failed to query items: %w", err)
	}
	defer rows.Close()

	var items []domain.Item
	for rows.Next() {
		var item domain.Item
		err := rows.Scan(
			&item.ChrtID, &item.TrackNumber, &item.Price, &item.Rid, &item.Name,
			&item.Sale, &item.Size, &item.TotalPrice, &item.NmID, &item.Brand, &item.Status,
		)
		if err != nil {
			log.Printf("Error scanning item: %v", err)
			continue
		}
		items = append(items, item)
	}

	return items, nil
}

// Проверка подключения
func (r *Repository) CheckConnection(ctx context.Context) error {
	var result int
	err := r.pool.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("connection check failed: %w", err)
	}
	log.Printf("✅ Database connection check: SELECT 1 = %d", result)
	return nil
}
