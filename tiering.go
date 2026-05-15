package storagetiering

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func init() {
	contracts.Register(func(deps contracts.ModuleDeps) contracts.Module {
		return &Module{
			deps: deps,
		}
	})
}

// Module implements automated storage tiering with configurable policies.
// It scans objects in the storage orchestrator and moves them between tiers
// based on age and prefix-matching policies. Tier is encoded in the key prefix:
// "hot/media/movie.mp4" -> "warm/media/movie.mp4" when relegated.
type Module struct {
	deps     contracts.ModuleDeps
	policies []TieringPolicy

	mu     sync.Mutex
	ticker *time.Ticker
	stopCh chan struct{}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:           "storage-tiering",
		Name:         "Storage Tiering",
		Version:      "1.0.0",
		Kinds:        []contracts.ModuleKind{contracts.ModuleKindProvider},
		Description:  "Automated storage tiering with configurable policies for hot/warm/cold data placement",
		Author:       "Muxcore-Media",
		Capabilities: []string{"storage.tiering", "storage.policy"},
	}
}

func (m *Module) Init(ctx context.Context) error {
	m.policies = loadPolicies()
	slog.Info("storage-tiering: loaded policies", "count", len(m.policies))
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	interval := defaultScanInterval()
	if v := os.Getenv("MUXCORE_TIERING_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}

	m.ticker = time.NewTicker(interval)
	m.stopCh = make(chan struct{})

	go m.runLoop()

	slog.Info("storage-tiering: started", "interval", interval)
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ticker != nil {
		m.ticker.Stop()
	}
	if m.stopCh != nil {
		close(m.stopCh)
	}

	slog.Info("storage-tiering: stopped")
	return nil
}

func (m *Module) Health(ctx context.Context) error { return nil }

// runLoop executes an initial scan on startup, then scans on the configured interval.
func (m *Module) runLoop() {
	m.scan(context.Background())

	for {
		select {
		case <-m.ticker.C:
			m.scan(context.Background())
		case <-m.stopCh:
			return
		}
	}
}

func defaultScanInterval() time.Duration {
	return 10 * time.Minute
}

// newEventID generates a random event ID without external dependencies.
func newEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
