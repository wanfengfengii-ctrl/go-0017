// Package report renders a reconciliation run as a deterministic JSON or CSV
// document. The report is sorted by currency, then discrepancy type, then
// stable record key, so that the same snapshot always yields byte-identical
// output. It includes the input batch IDs, rule revision, run time, summary
// amounts and per-item evidence.
package report

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

// Report is the serializable rendering of a run.
type Report struct {
	RunID         string               `json:"run_id"`
	GeneratedAt   time.Time            `json:"generated_at"`
	RuleRevision  int64                `json:"rule_revision"`
	BatchIDs      []string             `json:"batch_ids"`
	MatchGroups   []MatchGroupView     `json:"match_groups"`
	Discrepancies []DiscrepancyView    `json:"discrepancies"`
	Summary       *model.ReportSummary `json:"summary"`
}

// MatchGroupView is a stable rendering of a match group.
type MatchGroupView struct {
	ID          string       `json:"match_group_id"`
	Type        string       `json:"type"`
	Currency    string       `json:"currency"`
	AmountMinor int64        `json:"amount_minor"`
	Amount      string       `json:"amount"`
	Members     []MemberView `json:"members"`
	ScoreTuple  []int64      `json:"score_tuple"`
}

// MemberView renders one record in a match group.
type MemberView struct {
	RecordID     string `json:"record_id"`
	Role         string `json:"role"`
	ExternalID   string `json:"external_id"`
	Amount       string `json:"amount"`
	Currency     string `json:"currency"`
	TimestampUTC string `json:"timestamp_utc"`
}

// DiscrepancyView is a stable rendering of a discrepancy.
type DiscrepancyView struct {
	ID       string           `json:"discrepancy_id"`
	Type     string           `json:"type"`
	Currency string           `json:"currency"`
	Note     string           `json:"note,omitempty"`
	Evidence []model.Evidence `json:"evidence"`
	SortKey  string           `json:"sort_key"`
}

// Build assembles a deterministic report from a run result. recordsByID maps
// record IDs to records so member views can carry stable rendering details.
func Build(runID string, genAt time.Time, ruleRevision int64, batchIDs []string,
	matches []*model.MatchGroup, discreps []*model.Discrepancy,
	recordsByID map[string]*model.Record) *Report {

	bids := append([]string(nil), batchIDs...)
	sort.Strings(bids)

	mgViews := make([]MatchGroupView, 0, len(matches))
	for _, g := range matches {
		members := make([]MemberView, 0, len(g.RecordIDs))
		for _, rid := range g.RecordIDs {
			rec := recordsByID[rid]
			mv := MemberView{RecordID: rid}
			if rec != nil {
				mv.Role = string(rec.Role)
				mv.ExternalID = rec.ExternalID
				mv.Amount = money.Format(rec.Amount)
				mv.Currency = rec.Amount.C.Code
				mv.TimestampUTC = rec.Timestamp.UTC().Format(time.RFC3339Nano)
			}
			members = append(members, mv)
		}
		cur := g.Currency
		amtStr := ""
		if cur != "" {
			c, _ := money.NewRegistry().Lookup(cur)
			amtStr = money.Format(money.FromMinor(c, g.AmountMinor))
		}
		mgViews = append(mgViews, MatchGroupView{
			ID:          g.ID,
			Type:        string(g.Type),
			Currency:    cur,
			AmountMinor: g.AmountMinor,
			Amount:      amtStr,
			Members:     members,
			ScoreTuple:  append([]int64(nil), g.ScoreTuple...),
		})
	}
	sort.Slice(mgViews, func(i, j int) bool { return mgViews[i].ID < mgViews[j].ID })

	dvViews := make([]DiscrepancyView, 0, len(discreps))
	for _, d := range discreps {
		// Build a stable sort key from currency, type and the evidence record
		// stable keys (via recordsByID). If no record is found, fall back to ID.
		var keyParts []string
		keyParts = append(keyParts, d.Currency, string(d.Type))
		for _, ev := range d.Evidence {
			if rec := recordsByID[ev.RecordID]; rec != nil {
				keyParts = append(keyParts, rec.StableKey)
			} else {
				keyParts = append(keyParts, ev.RecordID)
			}
		}
		dvViews = append(dvViews, DiscrepancyView{
			ID:       d.ID,
			Type:     string(d.Type),
			Currency: d.Currency,
			Note:     d.Note,
			Evidence: append([]model.Evidence(nil), d.Evidence...),
			SortKey:  strings.Join(keyParts, "|"),
		})
	}
	// Sort by currency, then type, then stable record key (SortKey).
	sort.SliceStable(dvViews, func(i, j int) bool {
		if dvViews[i].Currency != dvViews[j].Currency {
			return dvViews[i].Currency < dvViews[j].Currency
		}
		if dvViews[i].Type != dvViews[j].Type {
			return dvViews[i].Type < dvViews[j].Type
		}
		return dvViews[i].SortKey < dvViews[j].SortKey
	})

	r := &Report{
		RunID:         runID,
		GeneratedAt:   genAt,
		RuleRevision:  ruleRevision,
		BatchIDs:      bids,
		MatchGroups:   mgViews,
		Discrepancies: dvViews,
	}
	r.Summary = summarize(r, matches, discreps)
	return r
}

func summarize(r *Report, matches []*model.MatchGroup, discreps []*model.Discrepancy) *model.ReportSummary {
	s := &model.ReportSummary{
		RunID:              r.RunID,
		MatchedCount:       len(matches),
		DiscrepancyCounts:  make(map[model.DiscType]int),
		DiscrepancyAmounts: make(map[model.DiscType]int64),
		ByCurrency:         make(map[string]model.CurrencySummary),
		GeneratedAt:        r.GeneratedAt,
		RuleRevision:       r.RuleRevision,
		BatchIDs:           r.BatchIDs,
	}
	for _, g := range matches {
		s.MatchedAmountMinor += g.AmountMinor
		cs := s.ByCurrency[g.Currency]
		cs.Currency = g.Currency
		cs.MatchedCount++
		cs.MatchedAmountMinor += g.AmountMinor
		s.ByCurrency[g.Currency] = cs
	}
	for _, d := range discreps {
		s.DiscrepancyCounts[d.Type]++
		cs := s.ByCurrency[d.Currency]
		cs.Currency = d.Currency
		cs.DiscrepancyCounts = ensureMap(cs.DiscrepancyCounts)
		cs.DiscrepancyAmounts = ensureAmtMap(cs.DiscrepancyAmounts)
		cs.DiscrepancyCounts[d.Type]++
		var amt int64
		for _, ev := range d.Evidence {
			amt += ev.AmountMinor
		}
		cs.DiscrepancyAmounts[d.Type] += amt
		s.DiscrepancyAmounts[d.Type] += amt
		s.ByCurrency[d.Currency] = cs
	}
	return s
}

func ensureMap(m map[model.DiscType]int) map[model.DiscType]int {
	if m == nil {
		return make(map[model.DiscType]int)
	}
	return m
}
func ensureAmtMap(m map[model.DiscType]int64) map[model.DiscType]int64 {
	if m == nil {
		return make(map[model.DiscType]int64)
	}
	return m
}

// MarshalJSON renders the report as deterministic JSON. Map keys are sorted
// (Go's json package sorts map keys alphabetically), and the top-level fields
// are already in a stable order, so two runs over the same snapshot produce
// byte-identical output.
func (r *Report) MarshalJSON() ([]byte, error) {
	type alias Report
	return json.Marshal((*alias)(r))
}

// JSON returns the deterministic JSON encoding with a trailing newline.
func (r *Report) JSON() ([]byte, error) {
	b, err := r.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// CSV returns a deterministic CSV rendering of the discrepancies (one row per
// discrepancy) followed by a summary section. The column order is fixed.
func (r *Report) CSV() ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	header := []string{"discrepancy_id", "type", "currency", "note", "evidence_record_ids", "sort_key"}
	if err := w.Write(header); err != nil {
		return nil, err
	}
	for _, d := range r.Discrepancies {
		var ids []string
		for _, ev := range d.Evidence {
			if ev.RecordID != "" {
				ids = append(ids, ev.RecordID)
			}
		}
		row := []string{d.ID, d.Type, d.Currency, d.Note, strings.Join(ids, ";"), d.SortKey}
		if err := w.Write(row); err != nil {
			return nil, err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	// Summary footer.
	fmt.Fprintf(&buf, "# summary matched=%d matched_amount_minor=%d discrepancies=%d rule_revision=%d\n",
		r.Summary.MatchedCount, r.Summary.MatchedAmountMinor, len(r.Discrepancies), r.RuleRevision)
	return buf.Bytes(), nil
}

// ContentDigest returns a stable hash over the report's business content (the
// sorted match-group IDs, discrepancy IDs and summary counts). Two runs over
// the same snapshot produce the same digest.
func (r *Report) ContentDigest() string {
	var sb strings.Builder
	mgIDs := make([]string, 0, len(r.MatchGroups))
	for _, g := range r.MatchGroups {
		mgIDs = append(mgIDs, g.ID)
	}
	sort.Strings(mgIDs)
	sb.WriteString("groups:")
	sb.WriteString(strings.Join(mgIDs, ","))
	dvIDs := make([]string, 0, len(r.Discrepancies))
	for _, d := range r.Discrepancies {
		dvIDs = append(dvIDs, d.ID)
	}
	sort.Strings(dvIDs)
	sb.WriteString("|discs:")
	sb.WriteString(strings.Join(dvIDs, ","))
	sb.WriteString(fmt.Sprintf("|matched=%d|amt=%d|rule=%d", r.Summary.MatchedCount, r.Summary.MatchedAmountMinor, r.RuleRevision))
	return sb.String()
}
