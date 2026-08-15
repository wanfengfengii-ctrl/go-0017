package matching

import (
	"fmt"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

// missingType returns the discrepancy type for a standalone record of role r
// given the rule's allowed roles. The missing type names the primary
// counterpart role that was absent.
func missingType(r model.Role, allowedRoles []model.Role) model.DiscType {
	counterpart := primaryCounterpart(r, allowedRoles)
	switch counterpart {
	case model.RoleInternal:
		return model.DiscMissingInternal
	case model.RoleProcessor:
		return model.DiscMissingProcessor
	case model.RoleBank:
		return model.DiscMissingBank
	default:
		return model.DiscMissingInternal
	}
}

func primaryCounterpart(r model.Role, allowedRoles []model.Role) model.Role {
	var priority []model.Role
	switch r {
	case model.RoleInternal:
		priority = []model.Role{model.RoleProcessor, model.RoleBank}
	case model.RoleProcessor:
		priority = []model.Role{model.RoleInternal, model.RoleBank}
	case model.RoleBank:
		priority = []model.Role{model.RoleProcessor, model.RoleInternal}
	default:
		priority = model.AllRoles
	}
	allowed := make(map[model.Role]bool, len(allowedRoles))
	for _, a := range allowedRoles {
		allowed[a] = true
	}
	for _, p := range priority {
		if p != r && allowed[p] {
			return p
		}
	}
	for _, role := range model.AllRoles {
		if role != r && allowed[role] {
			return role
		}
	}
	return r
}

func missingDiscTyped(r *model.Record, dtype model.DiscType) *model.Discrepancy {
	return &model.Discrepancy{
		ID:       discID(dtype, []string{r.ID}),
		Type:     dtype,
		Currency: r.Amount.C.Code,
		Evidence: []model.Evidence{recordEvidence(r)},
		Note:     fmt.Sprintf("no counterpart found for %s record %s", r.Role, r.ExternalID),
	}
}

func mismatchDisc(dtype model.DiscType, a, b *model.Record) *model.Discrepancy {
	ev := []model.Evidence{recordEvidence(a), recordEvidence(b)}
	return &model.Discrepancy{
		ID:       discID(dtype, []string{a.ID, b.ID}),
		Type:     dtype,
		Currency: a.Amount.C.Code,
		Evidence: ev,
		Note:     mismatchNote(dtype, a, b),
	}
}

func mismatchNote(dtype model.DiscType, a, b *model.Record) string {
	amountDiff, _ := money.AbsDiff(a.Amount, b.Amount)
	td := a.Timestamp.Sub(b.Timestamp)
	if td < 0 {
		td = -td
	}
	feeDiff, _ := money.AbsDiff(a.Fee, b.Fee)
	switch dtype {
	case model.DiscAmountMismatch:
		return fmt.Sprintf("amounts differ by %d minor units: %s=%s %s=%s", amountDiff, a.Role, money.Format(a.Amount), b.Role, money.Format(b.Amount))
	case model.DiscTimeMismatch:
		return fmt.Sprintf("timestamps differ by %s: %s=%s %s=%s", td, a.Role, a.TimestampRaw, b.Role, b.TimestampRaw)
	case model.DiscFeeMismatch:
		return fmt.Sprintf("fees differ by %d minor units: %s=%s %s=%s", feeDiff, a.Role, money.Format(a.Fee), b.Role, money.Format(b.Fee))
	default:
		return string(dtype)
	}
}

func searchLimitDisc(anchor *model.Record, cands []*model.Record, reason string) *model.Discrepancy {
	ev := []model.Evidence{recordEvidence(anchor)}
	for _, c := range cands {
		ev = append(ev, recordEvidence(c))
	}
	return &model.Discrepancy{
		ID:       discID(model.DiscSearchLimit, append([]string{anchor.ID}, sortedIDs(cands)...)),
		Type:     model.DiscSearchLimit,
		Currency: anchor.Amount.C.Code,
		Evidence: ev,
		Note:     reason,
	}
}

func ambiguousDisc(anchor *model.Record, partners []*model.Record, score []int64) *model.Discrepancy {
	ids := []string{anchor.ID}
	for _, p := range partners {
		ids = append(ids, p.ID)
	}
	cands := make([]model.CandidateEvidence, 0, len(partners))
	for _, p := range partners {
		cands = append(cands, model.CandidateEvidence{
			RecordID:     p.ID,
			ExternalID:   p.ExternalID,
			AmountString: money.Format(p.Amount),
			ScoreTuple:   score,
			StableKey:    p.StableKey,
		})
	}
	return &model.Discrepancy{
		ID:       discID(model.DiscAmbiguous, ids),
		Type:     model.DiscAmbiguous,
		Currency: anchor.Amount.C.Code,
		Evidence: []model.Evidence{{
			RecordID:     anchor.ID,
			Role:         anchor.Role,
			AmountMinor:  anchor.Amount.V,
			AmountString: money.Format(anchor.Amount),
			Currency:     anchor.Amount.C.Code,
			TimestampRaw: anchor.TimestampRaw,
			ExternalID:   anchor.ExternalID,
			Candidates:   cands,
		}},
		Note: "two or more candidates tied at the best score; cannot auto-resolve",
	}
}

func invalidRecordDisc(r *model.Record) *model.Discrepancy {
	return &model.Discrepancy{
		ID:       discID(model.DiscInvalidRecord, []string{r.ID}),
		Type:     model.DiscInvalidRecord,
		Currency: r.Amount.C.Code,
		Evidence: []model.Evidence{recordEvidence(r)},
		Note:     r.InvalidReason,
	}
}

func recordEvidence(r *model.Record) model.Evidence {
	ts := r.Timestamp
	return model.Evidence{
		RecordID:     r.ID,
		Role:         r.Role,
		AmountMinor:  r.Amount.V,
		AmountString: money.Format(r.Amount),
		Currency:     r.Amount.C.Code,
		Timestamp:    &ts,
		TimestampRaw: r.TimestampRaw,
		ExternalID:   r.ExternalID,
	}
}
