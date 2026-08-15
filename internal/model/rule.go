package model

import (
	"github.com/settlemesh/settlemesh/internal/money"
)

// RuleRevision is monotonic per rule set; a reconciliation run freezes a
// revision and is unaffected by later rule edits.
type RuleSet struct {
	Revision int64   `json:"revision"`
	Rules    []*Rule `json:"rules"`
}

// Rule binds a set of sources (by role) to matching tolerances and policies.
// A rule participates in a match group only when every member record's role is
// listed in AllowedRoles.
type Rule struct {
	ID                 string                `json:"rule_id"`
	Name               string                `json:"name"`
	AllowedRoles       []Role                `json:"allowed_roles"`
	BusinessIDFields   map[Role]string       `json:"business_id_fields"`   // role -> logical field for exact id join.
	BusinessIDPriority []string              `json:"business_id_priority"` // ordered logical field names.
	AmountTolerance    money.Tolerance       `json:"amount_tolerance"`
	FeeTolerance       money.Tolerance       `json:"fee_tolerance"`
	TimeWindow         int64                 `json:"time_window_ns"` // inclusive, in nanoseconds; 0 = exact.
	DupPolicy          map[DupType]DupAction `json:"dup_policy"`
	OneToMany          *OneToManyRule        `json:"one_to_many,omitempty"`
	SearchLimit        int                   `json:"search_limit"` // max combinations evaluated.
}

// OneToManyRule configures a bounded one-to-many match: one anchor record from
// AnchorRole matches up to MaxMembers records from ManyRole.
type OneToManyRule struct {
	AnchorRole Role `json:"anchor_role"`
	ManyRole   Role `json:"many_role"`
	MaxMembers int  `json:"max_members"`
}

// RuleConfig is the user-facing configuration document.
type RuleConfig struct {
	Revision int64             `json:"revision"`
	Rules    []RuleConfigEntry `json:"rules"`
}

type RuleConfigEntry struct {
	ID                 string            `json:"rule_id"`
	Name               string            `json:"name"`
	AllowedRoles       []Role            `json:"allowed_roles"`
	BusinessIDFields   map[string]string `json:"business_id_fields"`
	BusinessIDPriority []string          `json:"business_id_priority"`
	AmountTolerance    map[string]int64  `json:"amount_tolerance"` // {"abs":..., "pct_bps":...}
	FeeTolerance       map[string]int64  `json:"fee_tolerance"`
	TimeWindowSeconds  float64           `json:"time_window_seconds"` // converted to ns; stored as integer seconds*1e9.
	DupPolicy          map[string]string `json:"dup_policy"`
	OneToMany          *OneToManyConfig  `json:"one_to_many,omitempty"`
	SearchLimit        int               `json:"search_limit"`
}

type OneToManyConfig struct {
	AnchorRole Role `json:"anchor_role"`
	ManyRole   Role `json:"many_role"`
	MaxMembers int  `json:"max_members"`
}
