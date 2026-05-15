package storagetiering

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

// scan lists all objects from the storage orchestrator and evaluates each
// against the configured tiering policies. Objects that need to move are
// relocated via the orchestrator and a storage.tier.transition event is published.
func (m *Module) scan(ctx context.Context) {
	objects, err := m.deps.Storage.List(ctx, "")
	if err != nil {
		slog.Error("storage-tiering: scan list failed", "error", err)
		return
	}

	for _, obj := range objects {
		m.evaluateAndMove(ctx, obj)
	}
}

// evaluateAndMove checks all policies against a single object and executes
// the first matching policy that requires a tier transition.
func (m *Module) evaluateAndMove(ctx context.Context, obj contracts.ObjectInfo) {
	for _, policy := range m.policies {
		if !evaluatePolicy(policy, obj) {
			continue
		}

		currentTier := tierFromKey(obj.Key)
		newKey := newKeyInTier(obj.Key, policy.TargetTier)

		slog.Info("storage-tiering: moving object",
			"key", obj.Key,
			"tier", currentTier,
			"target", policy.TargetTier,
			"policy", policy.Name,
		)

		if err := m.deps.Storage.Move(ctx, obj.Key, newKey); err != nil {
			slog.Error("storage-tiering: move failed",
				"key", obj.Key,
				"target_tier", policy.TargetTier,
				"error", err,
			)
			continue
		}

		m.publishTransitionEvent(ctx, obj.Key, currentTier, policy)
	}
}

// publishTransitionEvent emits a storage.tier.transition event for the audit trail.
func (m *Module) publishTransitionEvent(ctx context.Context, key string, fromTier contracts.StorageTier, policy TieringPolicy) {
	payload, err := json.Marshal(contracts.TierTransitionPayload{
		Key:      key,
		FromTier: fromTier,
		ToTier:   policy.TargetTier,
		Reason:   fmt.Sprintf("policy %q: age >= %s", policy.Name, policy.MinAge.Duration()),
	})
	if err != nil {
		slog.Error("storage-tiering: failed to marshal event payload", "error", err)
		return
	}

	event := contracts.Event{
		ID:        newEventID(),
		Type:      contracts.EventStorageTierTransition,
		Source:    "storage-tiering",
		Payload:   payload,
		Metadata:  map[string]string{"policy": policy.Name},
	}

	if err := m.deps.EventBus.Publish(ctx, event); err != nil {
		slog.Error("storage-tiering: event publish failed",
			"event", event.Type,
			"policy", policy.Name,
			"error", err,
		)
	}
}
