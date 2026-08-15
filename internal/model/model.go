// Package model defines the core domain types for SettleMesh: sources, rules,
// normalized records, import batches, match groups, discrepancies and
// reconciliation runs. These types are shared across the ingest, dedup,
// matching, reconcile and report packages so that determinism guarantees hold
// end to end.
package model

import (
	"time"

	"github.com/settlemesh/settlemesh/internal/money"
)

// Role names the functional origin of a payment fact. Matching rules constrain
// which roles may participate in the same match group.
type Role string

const (
	RoleInternal  Role = "internal"
	RoleProcessor Role = "processor"
	RoleBank      Role = "bank"
)

// AllRoles is the canonical, stable ordering of roles used for deterministic
// iteration.
var AllRoles = []Role{RoleInternal, RoleProcessor, RoleBank}

// Source describes a configured ingestion source. A source pins a role, a
// currency, a timezone for interpreting naive timestamps, and the field map
// that translates raw feed columns to logical record fields.
type Source struct {
	ID       string                 `json:"source_id"`
	Name     string                 `json:"name"`
	Role     Role                   `json:"role"`
	Currency string                 `json:"currency"`
	TimeZone string                 `json:"timezone"`  // IANA name, e.g. "Asia/Shanghai".
	FieldMap map[string]string      `json:"field_map"` // logical -> raw column name.
	Format   string                 `json:"format"`    // "csv" or "ndjson".
	Config   map[string]interface{} `json:"config,omitempty"`
}

// FieldMapKey constants name the logical fields a record carries.
const (
	FieldExternalID    = "external_id"
	FieldAmount        = "amount"
	FieldCurrency      = "currency"
	FieldTimestamp     = "timestamp"
	FieldFee           = "fee"
	FieldDirection     = "direction"
	FieldBusinessID    = "business_id"
	FieldBusinessIDAlt = "business_id_alt"
	FieldCounterparty  = "counterparty"
	FieldMemo          = "memo"
)

// Record is a fully normalized payment fact. It is immutable once committed.
// StableKey uniquely and deterministically identifies a record across runs; it
// is derived from the source role, external id and content fingerprint.
type Record struct {
	ID            string       `json:"record_id"`
	SourceID      string       `json:"source_id"`
	Role          Role         `json:"role"`
	ExternalID    string       `json:"external_id"`
	BusinessID    string       `json:"business_id,omitempty"`
	Amount        money.Amount `json:"amount"`
	Fee           money.Amount `json:"fee"`
	Direction     string       `json:"direction"`     // "credit" or "debit" relative to the ledger.
	Timestamp     time.Time    `json:"timestamp"`     // UTC normalized.
	TimestampRaw  string       `json:"timestamp_raw"` // original text.
	Counterparty  string       `json:"counterparty,omitempty"`
	Memo          string       `json:"memo,omitempty"`
	SourceLine    int64        `json:"source_line"`  // stable 1-based line within the source feed.
	ContentHash   string       `json:"content_hash"` // content fingerprint, see dedup.
	StableKey     string       `json:"stable_key"`   // deterministic unique key.
	BatchID       string       `json:"batch_id"`
	RawLine       string       `json:"-"`                  // raw text, kept for evidence/debug, not serialized by default.
	Isolated      bool         `json:"isolated,omitempty"` // true if saved as an invalid isolated record.
	InvalidReason string       `json:"invalid_reason,omitempty"`
}

// Direction constants.
const (
	DirectionCredit = "credit"
	DirectionDebit  = "debit"
)

// RowError is a structured per-row validation failure carried through the
// batch state machine. It is defined here (rather than in ingest) so the store
// can persist it without importing the ingest package.
type RowError struct {
	Line   int64  `json:"line"`
	Field  string `json:"field"`
	Code   string `json:"code"`
	Reason string `json:"reason"`
	Value  string `json:"value,omitempty"`
}

// BatchStatus is the import batch lifecycle state.
type BatchStatus string

const (
	BatchReceiving  BatchStatus = "receiving"
	BatchValidating BatchStatus = "validating"
	BatchCommitted  BatchStatus = "committed"
	BatchFailed     BatchStatus = "failed"
	BatchCancelled  BatchStatus = "cancelled"
)

// RowErrorPolicy controls how row-level parse/validation errors affect a batch.
type RowErrorPolicy string

const (
	RowPolicyAtomic  RowErrorPolicy = "atomic"  // any invalid row rejects the whole batch.
	RowPolicyIsolate RowErrorPolicy = "isolate" // invalid rows are saved as isolated records; valid rows publish.
)

// BatchSummary is the deterministic, order-independent digest of a committed
// batch. It is what a reconciliation run freezes as its input snapshot.
type BatchSummary struct {
	BatchID         string `json:"batch_id"`
	SourceID        string `json:"source_id"`
	RecordCount     int    `json:"record_count"`
	ValidCount      int    `json:"valid_count"`
	InvalidCount    int    `json:"invalid_count"`
	AmountSumMinor  int64  `json:"amount_sum_minor"`
	AmountSumString string `json:"amount_sum_string"` // decimal string of the sum.
	ContentDigest   string `json:"content_digest"`    // hash over sorted record content.
}

// Batch is an import batch.
type Batch struct {
	ID             string         `json:"batch_id"`
	SourceID       string         `json:"source_id"`
	Status         BatchStatus    `json:"status"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	FileDigest     string         `json:"file_digest"`
	RowPolicy      RowErrorPolicy `json:"row_policy"`
	Summary        *BatchSummary  `json:"summary,omitempty"`
	Records        []*Record      `json:"records,omitempty"` // populated for committed batches in memory.
	CreatedAt      time.Time      `json:"created_at"`
	CommittedAt    *time.Time     `json:"committed_at,omitempty"`
	Revision       int64          `json:"revision"` // increments on each committed transition.
}

// DupType classifies a duplicate relationship.
type DupType string

const (
	DupExactKey           DupType = "exact_key"           // same external_id within a source.
	DupRetransmit         DupType = "retransmit"          // same file digest re-uploaded.
	DupContentFingerprint DupType = "content_fingerprint" // same content, different external_id.
	DupSuspected          DupType = "suspected"           // heuristic near-match.
)

// DupAction is what the rule says to do with a duplicate.
type DupAction string

const (
	DupExclude DupAction = "exclude" // drop the duplicate, keep canonical.
	DupReport  DupAction = "report"  // keep but report as a discrepancy.
)

// DuplicateRelation links a duplicate record to its canonical record.
type DuplicateRelation struct {
	ID                string    `json:"relation_id"`
	Type              DupType   `json:"type"`
	CanonicalRecordID string    `json:"canonical_record_id"`
	DuplicateRecordID string    `json:"duplicate_record_id"`
	SourceID          string    `json:"source_id"`
	Action            DupAction `json:"action"`
}

// MatchType classifies how a match group was formed.
type MatchType string

const (
	MatchExactID   MatchType = "exact_id"
	MatchTolerance MatchType = "tolerance"
	MatchOneToMany MatchType = "one_to_many"
)

// MatchGroup is an immutable reconciliation conclusion linking one or more
// records across sources.
type MatchGroup struct {
	ID          string    `json:"match_group_id"`
	RunID       string    `json:"run_id"`
	Type        MatchType `json:"type"`
	Role        Role      `json:"anchor_role,omitempty"` // for one_to_many, the single-member role.
	RecordIDs   []string  `json:"record_ids"`            // stable-sorted.
	ScoreTuple  []int64   `json:"score_tuple"`           // [tier, amountDiff, timeDiff, feeDiff].
	AmountMinor int64     `json:"amount_minor"`
	Currency    string    `json:"currency"`
}

// DiscType is the discrepancy classification taxonomy.
type DiscType string

const (
	DiscMissingInternal  DiscType = "missing_internal"
	DiscMissingProcessor DiscType = "missing_processor"
	DiscMissingBank      DiscType = "missing_bank"
	DiscAmountMismatch   DiscType = "amount_mismatch"
	DiscTimeMismatch     DiscType = "time_mismatch"
	DiscFeeMismatch      DiscType = "fee_mismatch"
	DiscCurrencyMismatch DiscType = "currency_mismatch"
	DiscDuplicate        DiscType = "duplicate"
	DiscAmbiguous        DiscType = "ambiguous"
	DiscInvalidRecord    DiscType = "invalid_record"
	DiscSearchLimit      DiscType = "search_limit"
)

// Evidence captures the per-item detail attached to a discrepancy.
type Evidence struct {
	RecordID      string              `json:"record_id,omitempty"`
	Role          Role                `json:"role,omitempty"`
	AmountMinor   int64               `json:"amount_minor,omitempty"`
	AmountString  string              `json:"amount_string,omitempty"`
	Currency      string              `json:"currency,omitempty"`
	Timestamp     *time.Time          `json:"timestamp,omitempty"`
	TimestampRaw  string              `json:"timestamp_raw,omitempty"`
	ExternalID    string              `json:"external_id,omitempty"`
	Candidates    []CandidateEvidence `json:"candidates,omitempty"`
	ToleranceNote string              `json:"tolerance_note,omitempty"`
}

// CandidateEvidence describes one candidate considered for an ambiguous or
// search_limit discrepancy.
type CandidateEvidence struct {
	RecordID     string  `json:"record_id"`
	ExternalID   string  `json:"external_id"`
	AmountString string  `json:"amount_string"`
	ScoreTuple   []int64 `json:"score_tuple"`
	StableKey    string  `json:"stable_key"`
}

// Discrepancy is an unmatched or mismatched financial fact that cannot be
// auto-resolved. Discrepancy IDs are deterministic (hash of type + sorted
// evidence record ids) so they are stable across reruns and worker counts.
type Discrepancy struct {
	ID       string     `json:"discrepancy_id"`
	RunID    string     `json:"run_id"`
	Type     DiscType   `json:"type"`
	Currency string     `json:"currency"`
	Evidence []Evidence `json:"evidence"`
	Note     string     `json:"note,omitempty"`
}

// RunStatus is the reconciliation run lifecycle.
type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

// RunSnapshot fixes the inputs to a reconciliation run.
type RunSnapshot struct {
	RunID          string         `json:"run_id"`
	BatchIDs       []string       `json:"batch_ids"`
	BatchSummaries []BatchSummary `json:"batch_summaries"`
	RuleRevision   int64          `json:"rule_revision"`
	CreatedAt      time.Time      `json:"created_at"`
}

// Run is a reconciliation execution.
type Run struct {
	ID            string         `json:"run_id"`
	Status        RunStatus      `json:"status"`
	Snapshot      *RunSnapshot   `json:"snapshot,omitempty"`
	StartedAt     *time.Time     `json:"started_at,omitempty"`
	FinishedAt    *time.Time     `json:"finished_at,omitempty"`
	MatchGroups   []*MatchGroup  `json:"match_groups,omitempty"`
	Discrepancies []*Discrepancy `json:"discrepancies,omitempty"`
	Summary       *ReportSummary `json:"summary,omitempty"`
	FaultPoint    string         `json:"fault_point,omitempty"` // if failed at an injected fault.
}

// ReportSummary holds aggregate counts and amounts for a run report.
type ReportSummary struct {
	RunID              string                     `json:"run_id"`
	MatchedCount       int                        `json:"matched_count"`
	MatchedAmountMinor int64                      `json:"matched_amount_minor"`
	DiscrepancyCounts  map[DiscType]int           `json:"discrepancy_counts"`
	DiscrepancyAmounts map[DiscType]int64         `json:"discrepancy_amounts"`
	ByCurrency         map[string]CurrencySummary `json:"by_currency"`
	GeneratedAt        time.Time                  `json:"generated_at"`
	RuleRevision       int64                      `json:"rule_revision"`
	BatchIDs           []string                   `json:"batch_ids"`
}

// CurrencySummary breaks down a single currency's results.
type CurrencySummary struct {
	Currency           string             `json:"currency"`
	MatchedCount       int                `json:"matched_count"`
	MatchedAmountMinor int64              `json:"matched_amount_minor"`
	DiscrepancyCounts  map[DiscType]int   `json:"discrepancy_counts"`
	DiscrepancyAmounts map[DiscType]int64 `json:"discrepancy_amounts"`
}
