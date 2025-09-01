package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"order-service/internal/service"
	"path/filepath"
	"runtime"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Handler struct {
	service *service.Service
}

func New(service *service.Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) InitRoutes() http.Handler {
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Обслуживание статических файлов из корневой директории
	r.Get("/", h.serveIndex)
	r.Get("/index.html", h.serveIndex)

	// API
	r.Route("/api", func(r chi.Router) {
		r.Get("/health", h.healthCheck)
		r.Route("/orders", func(r chi.Router) {
			r.Get("/", h.getAllOrders)
			r.Get("/{orderUID}", h.getOrderByUID)
		})
	})

	return r
}

// index.html
func (h *Handler) serveIndex(w http.ResponseWriter, r *http.Request) {
	_, filename, _, _ := runtime.Caller(0)
	rootDir := filepath.Join(filepath.Dir(filename), "..", "..")
	staticDir := filepath.Join(rootDir, "static")
	indexPath := filepath.Join(staticDir, "index.html")

	http.ServeFile(w, r, indexPath)
}

// Проверка состояния
func (h *Handler) healthCheck(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Получение всех заказов из кеша
func (h *Handler) getAllOrders(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"message": "Not implemented yet"})
}

func (h *Handler) getOrderByUID(w http.ResponseWriter, r *http.Request) {
	orderUID := chi.URLParam(r, "orderUID")

	if orderUID == "" {
		errorResponse(w, http.StatusBadRequest, "Order UID is required")
		return
	}

	order, err := h.service.GetOrder(r.Context(), orderUID)
	if err != nil {
		errorResponse(w, http.StatusNotFound, "Order not found")
		return
	}

	jsonResponse(w, http.StatusOK, order)
}

func jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error encoding JSON response: %v", err)
	}
}

func errorResponse(w http.ResponseWriter, status int, message string) {
	jsonResponse(w, status, map[string]string{"error": message})
}
