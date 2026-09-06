package service

import (
	"context"
	"errors"
	"strings"

	"github.com/oarkflow/fh/examples/template/internal/repository"
)

var ErrInvalidName = errors.New("name is required")

type Service struct{ repo repository.Repository }

func New(repo repository.Repository) *Service                          { return &Service{repo: repo} }
func (s *Service) List(ctx context.Context) ([]repository.Item, error) { return s.repo.List(ctx) }
func (s *Service) Add(ctx context.Context, name string) (repository.Item, error) {
	if strings.TrimSpace(name) == "" {
		return repository.Item{}, ErrInvalidName
	}
	return s.repo.Add(ctx, strings.TrimSpace(name))
}
