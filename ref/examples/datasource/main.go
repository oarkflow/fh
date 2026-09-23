package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/observer"
	"github.com/oarkflow/fh/ref/runtime"
	"github.com/oarkflow/fh/ref/source"
)

// --- Domain Models ---

type User struct {
	ID   int64
	Name string
}

type TenantSettings struct {
	TenantID string
	Theme    string
}

// DashboardResponse is what the intent returns
type DashboardResponse struct {
	Settings TenantSettings  `json:"settings"`
	Users    map[int64]User  `json:"users"`
}

// DashboardRequest is the incoming payload
type DashboardRequest struct {
	TenantID string  `json:"tenant_id"`
	UIDs     []int64 `json:"uids"`
}

// --- Fact Keys ---

var (
	KeyDashboardReq   = fact.NewKey[DashboardRequest]("dashboard.request")
	KeyUserResult     = fact.NewKey[map[int64]User]("users.result")
	KeyTenantSettings = fact.NewKey[TenantSettings]("tenant.settings")
)

// --- Mock Database / API ---

var dbQueryCount atomic.Int32
var apiCallCount atomic.Int32

func mockDBFetchUsers(ctx context.Context, ids []int64) (map[int64]User, error) {
	dbQueryCount.Add(1)
	log.Printf("[DB] Executing query for %d users: %v", len(ids), ids)
	time.Sleep(50 * time.Millisecond) // Simulate DB latency

	results := make(map[int64]User)
	for _, id := range ids {
		results[id] = User{ID: id, Name: fmt.Sprintf("User_%d", id)}
	}
	return results, nil
}

func mockAPIFetchSettings(ctx context.Context, tenantID string) (TenantSettings, error) {
	apiCallCount.Add(1)
	log.Printf("[API] Fetching settings for tenant: %s", tenantID)
	time.Sleep(100 * time.Millisecond) // Simulate API latency

	return TenantSettings{TenantID: tenantID, Theme: "dark"}, nil
}

// --- Custom Observer for Metrics ---

type metricsObserver struct {
	observer.Noop
	collector *source.MetricsCollector
	mu        sync.Mutex
}

func (m *metricsObserver) SourceFetched(metrics observer.SourceMetrics) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Map observer.SourceMetrics to source.Metrics
	sm := source.Metrics{
		SourceName:  metrics.SourceName,
		Operation:   metrics.Operation,
		QueryHash:   metrics.QueryHash,
		QueueWait:   metrics.QueueWait,
		ConnWait:    metrics.ConnWait,
		ExecTime:    metrics.ExecTime,
		DecodeTime:  metrics.DecodeTime,
		TotalTime:   metrics.TotalTime,
		ResultCount: metrics.ResultCount,
		ResultBytes: metrics.ResultBytes,
		CacheHit:    metrics.CacheHit,
		CacheLevel:  metrics.CacheLevel,
		Batched:     metrics.Batched,
		BatchSize:   metrics.BatchSize,
		Coalesced:   metrics.Coalesced,
		Error:       metrics.Error,
	}
	m.collector.Record(sm)
}

// adapt converts a source.Registration into a capability.Registration
func adapt(sr source.Registration) capability.Registration {
	return capability.Registration{
		Name:        sr.Name,
		Requires:    sr.Requires,
		Provides:    sr.Provides,
		Kind:        sr.Kind,
		Speculation: sr.Speculation,
		Source:      sr.Source,
		Run:         sr.Run,
	}
}

func main() {
	fmt.Println("=== REF DataSource Subsystem Showcase ===")
	fmt.Println("Showcasing: DataLoader (Batching), L1 Caching, and Coalescing")
	fmt.Println()

	// 1. Setup Caching and Metrics
	cache := source.NewProcessCache(source.ProcessCacheConfig{MaxEntries: 1000})
	coalescer := source.NewCoalescer()
	metricsColl := source.NewMetricsCollector()
	metricsObs := &metricsObserver{collector: metricsColl}
	compObs := observer.NewCompositeObserver(nil, observer.TieredObserver{Observer: metricsObs, Tier: observer.Critical})

	// 2. Define Capabilities

	// Capability A: Batched User Fetcher (Database)
	// We use source.NewLoader manually inside the capability to simulate multiple
	// concurrent ReadNode executions (e.g. from a list of GraphQL field resolvers)
	// collapsing into a single DataLoader batch.
	userLoader := source.NewLoader[int64, User](mockDBFetchUsers, source.LoaderConfig{
		Wait:     10 * time.Millisecond,
		MaxBatch: 100,
	})

	nPlusOneResolver := capability.NewRegistration(
		"capability.resolve_users_batched",
		graph.ReadNode,
		capability.WithSource(&source.Spec{
			Name:      "users-db",
			Kind:      source.KindDatabase,
			Batchable: true,
		}),
	).WithRequires(KeyDashboardReq.Any()).
		WithProvides(KeyUserResult.Any()).
		WithRun(func(nc *execution.NodeContext) error {
			start := time.Now()
			slot, _ := nc.SlotOf(KeyDashboardReq.DefinitionID())
			req, _ := fact.Get[DashboardRequest](nc.Facts(), slot)
			
			// Simulate N concurrent graph nodes needing individual users (N+1 scenario)
			var wg sync.WaitGroup
			var mu sync.Mutex
			results := make(map[int64]User)

			for _, uid := range req.UIDs {
				wg.Add(1)
				go func(id int64) {
					defer wg.Done()
					// 🚀 THIS IS THE MAGIC: N concurrent Loads collapse into 1 batch call
					user, err := userLoader.Load(context.Background(), id)
					if err == nil {
						mu.Lock()
						results[id] = user
						mu.Unlock()
					}
				}(uid)
			}
			wg.Wait()

			metricsObs.SourceFetched(observer.SourceMetrics{
				SourceName: "users-db",
				Operation:  "batch_fetch",
				Batched:    true,
				BatchSize:  len(req.UIDs),
				ExecTime:   time.Since(start),
				TotalTime:  time.Since(start),
			})

			outSlot, _ := nc.SlotOf(KeyUserResult.DefinitionID())
			fact.Put(nc.Facts(), outSlot, results)
			return nil
		})

	// Capability B: Cached & Coalesced Tenant Settings Fetcher (API)
	settingsFetcherReg := source.NewFetchCapability(
		"capability.fetch_tenant_settings",
		source.Spec{
			Name:        "tenant-api",
			Kind:        source.KindAPI,
			ReadOnly:    true,
			Cacheable:   true,
			Coalescible: true,
			CacheTTL:    5 * time.Minute,
			CacheScope:  source.ScopeProcess, // L1 Cache
			Consistency: source.Eventual,
		},
		KeyTenantSettings.Any(),
		func(nc *execution.NodeContext) (any, error) {
			slot, _ := nc.SlotOf(KeyDashboardReq.DefinitionID())
			req, _ := fact.Get[DashboardRequest](nc.Facts(), slot)
			return mockAPIFetchSettings(context.Background(), req.TenantID)
		},
		source.WithCache(cache),
		source.WithCoalescer(coalescer),
		source.WithKeyFunc(func(nc *execution.NodeContext) string {
			slot, _ := nc.SlotOf(KeyDashboardReq.DefinitionID())
			req, _ := fact.Get[DashboardRequest](nc.Facts(), slot)
			return req.TenantID
		}),
		source.WithMetricsObserver(func(sm source.Metrics) {
			metricsColl.Record(sm)
		}),
	)
	settingsFetcherReg.Requires = []fact.AnyKey{KeyDashboardReq.Any()} // Fix DAG race condition
	settingsFetcher := adapt(settingsFetcherReg)

	// 3. Define the Dashboard Intent
	dashboardIntent := &intent.Definition{
		Name: "LoadDashboardData",
		Spec: intent.Spec{
			Requires: []fact.AnyKey{
				KeyUserResult.Any(),
				KeyTenantSettings.Any(),
			},
		},
		InputKey: KeyDashboardReq.Any(),
		DecodeNode: func(nc *execution.NodeContext) error {
			var req DashboardRequest
			if err := json.Unmarshal(nc.Invocation().Input.RawBytes(), &req); err != nil {
				return err
			}
			slot, _ := nc.SlotOf(KeyDashboardReq.DefinitionID())
			fact.Put(nc.Facts(), slot, req)
			return nil
		},
		Run: func(nc *execution.NodeContext) (any, effect.EffectPlan, intent.OutcomeMeta, error) {
			s1, _ := nc.SlotOf(KeyUserResult.DefinitionID())
			users, _ := fact.Get[map[int64]User](nc.Facts(), s1)
			
			s2, _ := nc.SlotOf(KeyTenantSettings.DefinitionID())
			settings, _ := fact.Get[TenantSettings](nc.Facts(), s2)

			res := DashboardResponse{
				Users:    users,
				Settings: settings,
			}
			return res, effect.EffectPlan{}, intent.OutcomeMeta{}, nil
		},
	}

	engine := runtime.NewEngine(
		runtime.WithCapability(nPlusOneResolver),
		runtime.WithCapability(settingsFetcher),
		runtime.WithObserver(compObs),
	)
	if err := engine.RegisterDefinition(dashboardIntent); err != nil {
		log.Fatalf("RegisterDefinition failed: %v", err)
	}
	if err := engine.Compile(); err != nil {
		log.Fatalf("Compile failed: %v", err)
	}

	// Helper to run intents
	runIntent := func(req DashboardRequest) {
		payload, _ := json.Marshal(req)
		inv := &invocation.Invocation{
			ID:     "inv-1",
			Intent: "LoadDashboardData",
			Input:  invocation.NewInputDirect(payload, "application/json"),
		}
		res, err := engine.Dispatch(context.Background(), inv)
		if err != nil {
			log.Fatalf("Dispatch failed: %v", err)
		}
		_ = res // In a real app we'd serialize this to HTTP
	}

	// === SCENARIO 1: First Request (Cold Cache, simulates N+1) ===
	fmt.Println("\n--- Scenario 1: First Request (Cold Cache, 5 concurrent user loads) ---")
	runIntent(DashboardRequest{
		TenantID: "tenant-acme",
		UIDs:     []int64{101, 102, 103, 104, 105},
	})
	fmt.Printf("Database Queries Executed: %d (Expected: 1, due to DataLoader batching!)\n", dbQueryCount.Load())
	fmt.Printf("API Calls Executed: %d (Expected: 1)\n", apiCallCount.Load())

	// === SCENARIO 2: Second Request (Hot Cache) ===
	fmt.Println("\n--- Scenario 2: Second Request (Hot Cache for Tenant API) ---")
	runIntent(DashboardRequest{
		TenantID: "tenant-acme", // Same tenant
		UIDs:     []int64{201, 202}, // Different users
	})
	fmt.Printf("Database Queries Executed: %d (Expected: 2, new users fetched in 1 batch)\n", dbQueryCount.Load())
	fmt.Printf("API Calls Executed: %d (Expected: 1, Served from ProcessCache!)\n", apiCallCount.Load())

	// === SCENARIO 3: High Concurrency Thundering Herd (Coalescing) ===
	fmt.Println("\n--- Scenario 3: Thundering Herd (100 simultaneous requests for same settings) ---")
	apiCallCount.Store(0)
	cache.InvalidateSource("tenant-api") // Clear cache to force coalescing logic
	
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runIntent(DashboardRequest{
				TenantID: "tenant-stark",
				UIDs:     []int64{999},
			})
		}()
	}
	wg.Wait()
	
	fmt.Printf("API Calls Executed for 100 concurrent requests: %d (Expected: 1, due to Coalescer!)\n", apiCallCount.Load())

	// Print Metrics Summary
	fmt.Println("\n--- Source Metrics Summary ---")
	summary := metricsColl.Summary()
	for sourceName, stats := range summary.BySource {
		fmt.Printf("Source: %-12s | Ops: %2d | Cache Hits: %2d | Avg Latency: %v\n", 
			sourceName, stats.Ops, stats.CacheHits, 
			func() time.Duration {
				if stats.Ops == 0 { return 0 }
				return stats.TotalTime / time.Duration(stats.Ops)
			}())
	}
}
