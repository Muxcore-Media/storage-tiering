package storagetiering

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

// Duration wraps time.Duration to marshal/unmarshal as human-readable strings in JSON.
// Examples: "24h", "168h", "720h".
type Duration time.Duration

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(dur)
	return nil
}

// Duration returns the underlying time.Duration value.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// TieringPolicy defines when objects matching a key prefix should be moved to a target tier.
type TieringPolicy struct {
	Name       string              `json:"name"`
	Prefix     string              `json:"prefix"`
	MinAge     Duration            `json:"min_age"`
	TargetTier contracts.StorageTier `json:"target_tier"`
	Enabled    bool                `json:"enabled"`
}

// defaultPolicies returns the built-in default tiering policies.
func defaultPolicies() []TieringPolicy {
	return []TieringPolicy{
		{
			Name:       "media-hot",
			Prefix:     "media/",
			MinAge:     Duration(24 * time.Hour),
			TargetTier: contracts.StorageTierWarm,
			Enabled:    true,
		},
		{
			Name:       "media-warm",
			Prefix:     "media/",
			MinAge:     Duration(7 * 24 * time.Hour),
			TargetTier: contracts.StorageTierCold,
			Enabled:    true,
		},
		{
			Name:       "backups-cold",
			Prefix:     "backups/",
			MinAge:     Duration(30 * 24 * time.Hour),
			TargetTier: contracts.StorageTierArchive,
			Enabled:    true,
		},
	}
}

// loadPolicies loads policies from the MUXCORE_TIERING_POLICIES env var.
// Falls back to defaultPolicies() if the env var is empty or invalid.
func loadPolicies() []TieringPolicy {
	raw := os.Getenv("MUXCORE_TIERING_POLICIES")
	if raw == "" {
		return defaultPolicies()
	}

	var policies []TieringPolicy
	if err := json.Unmarshal([]byte(raw), &policies); err != nil {
		return defaultPolicies()
	}

	return policies
}

// evaluatePolicy returns true if the object should be moved to the policy's target tier.
func evaluatePolicy(policy TieringPolicy, info contracts.ObjectInfo) bool {
	if !policy.Enabled {
		return false
	}

	// Check whether the content prefix matches the policy prefix.
	// For a key like "hot/media/movie.mp4", the content prefix is "media/movie.mp4".
	contentPrefix := keyPrefixFromKey(info.Key)
	if !strings.HasPrefix(contentPrefix, policy.Prefix) {
		return false
	}

	// Check whether the object has aged past the policy threshold.
	age := time.Since(time.Unix(info.LastModified, 0))
	if age < policy.MinAge.Duration() {
		return false
	}

	// Determine the current tier. If already in the target tier, nothing to do.
	currentTier := tierFromKey(info.Key)
	if currentTier == policy.TargetTier {
		return false
	}

	// Only allow moves to a colder tier — tiering is about relegation, not promotion.
	if isHigherTier(policy.TargetTier, currentTier) {
		return false
	}

	return true
}

// isHigherTier returns true when tier a is hotter (higher priority) than tier b.
// Hierarchy: hot (1) > warm (2) > cold (3) > archive (4)
func isHigherTier(a, b contracts.StorageTier) bool {
	rank := map[contracts.StorageTier]int{
		contracts.StorageTierHot:     1,
		contracts.StorageTierWarm:    2,
		contracts.StorageTierCold:    3,
		contracts.StorageTierArchive: 4,
	}
	return rank[a] < rank[b]
}

// tierFromKey extracts the storage tier from the key prefix.
// Returns StorageTierHot as the default when no recognized prefix is found.
func tierFromKey(key string) contracts.StorageTier {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) < 2 {
		return contracts.StorageTierHot
	}
	switch parts[0] {
	case "hot", "warm", "cold", "archive":
		return contracts.StorageTier(parts[0])
	default:
		return contracts.StorageTierHot
	}
}

// keyPrefixFromKey strips the tier prefix from a key, returning the content path.
// e.g. "hot/media/movie.mp4" -> "media/movie.mp4"
func keyPrefixFromKey(key string) string {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) < 2 {
		return key
	}
	switch parts[0] {
	case "hot", "warm", "cold", "archive":
		return parts[1]
	default:
		return key
	}
}

// newKeyInTier builds a new key with the given tier prefix.
// e.g. ("hot/media/movie.mp4", "cold") -> "cold/media/movie.mp4"
func newKeyInTier(key string, tier contracts.StorageTier) string {
	return string(tier) + "/" + keyPrefixFromKey(key)
}
