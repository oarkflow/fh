package repository

import (
	"context"
	"sync"
)

type Repository interface {
	Health(context.Context) error
	List(context.Context) ([]Item, error)
	Add(context.Context, string) (Item, error)
}
type Item struct {
	ID   int    `json:"id" db:"id"`
	Name string `json:"name" db:"name"`
}
type Memory struct {
	mu    sync.RWMutex
	next  int
	items []Item
}

func NewMemory() *Memory                       { return &Memory{next: 1} }
func (m *Memory) Health(context.Context) error { return nil }
func (m *Memory) List(context.Context) ([]Item, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]Item{}, m.items...), nil
}
func (m *Memory) Add(_ context.Context, name string) (Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item := Item{ID: m.next, Name: name}
	m.next++
	m.items = append(m.items, item)
	return item, nil
}
