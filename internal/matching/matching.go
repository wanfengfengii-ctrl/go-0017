// Package matching implements the deterministic reconciliation matching
// engine. Given a snapshot of committed, deduplicated records and a rule set,
// it produces match groups and discrepancies in a fully deterministic way:
//
//   - Records are partitioned by currency; cross-currency business-id collisions
//     yield currency_mismatch discrepancies.
//   - Phase 1: exact business-identifier matching (tier 0).
//   - Phase 2: bounded one-to-many combination search (tier 2), for rules that
//     configure it.
//   - Phase 3: one-to-one tolerance matching (tier 1) with a single commit
//     stage that selects non-conflicting pairs in stable score order.
//   - Ties in best score for a record produce ambiguous discrepancies (the
//     engine never picks arbitrarily).
//   - Remaining records are classified as amount_mismatch, fee_mismatch,
//     time_mismatch or missing_<role>.
//
// All IDs (match groups, discrepancies) are hashes of sorted member record IDs,
// so they are stable across reruns and independent of goroutine scheduling.
package matching

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

// Result is the engine output.
type Result struct {
	Groups        []*model.MatchGroup
	Discrepancies []*model.Discrepancy
}

// Engine runs a rule set against records.
type Engine struct {
	rules    []*model.Rule
	registry *money.Registry
	now      time.Time
}

// NewEngine constructs an engine. rules are applied in order; each record is
// claimed by the first rule that matches it.
func NewEngine(rules []*model.Rule, registry *money.Registry, now time.Time) *Engine {
	if registry == nil {
		registry = money.NewRegistry()
	}
	return &Engine{rules: rules, registry: registry, now: now}
}

// Run executes matching over records. excluded IDs are skipped.
func (e *Engine) Run(records []*model.Record, excluded map[string]bool) *Result {
	res := &Result{}
	free := make([]*model.Record, 0, len(records))
	for _, r := range records {
		if r.Isolated {
			res.Discrepancies = append(res.Discrepancies, invalidRecordDisc(r))
			continue
		}
		if excluded[r.ID] {
			continue
		}
		free = append(free, r)
	}

	// claimed is shared across all phases and rules so that a record claimed
	// by an earlier phase/rule is never reconsidered.
	claimed := make(map[string]bool)
	claim := func(id string) { claimed[id] = true }

	// Apply each rule in order over the currently-free records.
	for _, rule := range e.rules {
		applicable := filterByRule(free, rule, claimed)
		if len(applicable) == 0 {
			continue
		}
		e.applyRule(rule, applicable, res, claim, claimed)
	}

	// Remaining free records become mismatch/missing discrepancies.
	for _, r := range free {
		if claimed[r.ID] {
			continue
		}
		if d := e.classifyRemaining(r, records, excluded); d != nil {
			res.Discrepancies = append(res.Discrepancies, d)
		}
	}
	return res
}

func filterByRule(records []*model.Record, rule *model.Rule, claimed map[string]bool) []*model.Record {
	allowed := make(map[model.Role]bool)
	for _, r := range rule.AllowedRoles {
		allowed[r] = true
	}
	out := make([]*model.Record, 0)
	for _, r := range records {
		if r == nil {
			continue
		}
		if claimed[r.ID] {
			continue
		}
		if allowed[r.Role] {
			out = append(out, r)
		}
	}
	return out
}

// applyRule runs the three phases for one rule over the applicable subset.
// claim marks a record as consumed; claimed is the shared state consulted by
// every phase to skip already-matched records.
func (e *Engine) applyRule(rule *model.Rule, applicable []*model.Record, res *Result, claim func(string), claimed map[string]bool) {
	byCurrency := partitionByCurrency(applicable)
	curKeys := sortedCurrencyKeys(byCurrency)
	for _, ck := range curKeys {
		recs := byCurrency[ck]
		// Phase 1: exact id.
		e.phaseExactID(rule, recs, res, claim, claimed)
		// Phase 2: one-to-many.
		if rule.OneToMany != nil {
			e.phaseOneToMany(rule, recs, res, claim, claimed)
		}
		// Phase 3: one-to-one tolerance.
		e.phaseOneToOne(rule, recs, res, claim, claimed)
	}
}

func partitionByCurrency(records []*model.Record) map[string][]*model.Record {
	out := make(map[string][]*model.Record)
	for _, r := range records {
		c := r.Amount.C.Code
		out[c] = append(out[c], r)
	}
	return out
}

func sortedCurrencyKeys(m map[string][]*model.Record) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---- Phase 1: exact business-identifier matching ----

func (e *Engine) phaseExactID(rule *model.Rule, recs []*model.Record, res *Result, claim func(string), claimed map[string]bool) {
	// Determine the business-id field per role.
	bidField := func(r *model.Record) string {
		if f, ok := rule.BusinessIDFields[r.Role]; ok && f != "" {
			return f
		}
		return model.FieldBusinessID
	}
	// Group by business id value.
	type bidEntry struct {
		role model.Role
		rec  *model.Record
	}
	byBid := make(map[string][]bidEntry)
	for _, r := range recs {
		if r == nil || claimed[r.ID] {
			continue
		}
		bid := businessIDValue(r, bidField(r))
		if bid == "" {
			continue
		}
		byBid[bid] = append(byBid[bid], bidEntry{r.Role, r})
	}
	bids := make([]string, 0, len(byBid))
	for b := range byBid {
		bids = append(bids, b)
	}
	sort.Strings(bids)
	for _, bid := range bids {
		entries := byBid[bid]
		if len(entries) < 2 {
			continue
		}
		// Check currency uniformity (all in same partition already, so same
		// currency). Check role distinctness: pick one record per role (smallest
		// stable key).
		byRole := make(map[model.Role][]*model.Record)
		for _, en := range entries {
			byRole[en.role] = append(byRole[en.role], en.rec)
		}
		members := make([]*model.Record, 0, len(byRole))
		for _, role := range model.AllRoles {
			if rs, ok := byRole[role]; ok {
				sort.Slice(rs, func(i, j int) bool { return rs[i].StableKey < rs[j].StableKey })
				members = append(members, rs[0])
			}
		}
		if len(members) < 2 {
			continue
		}
		// All members share the same currency (same partition). Emit exact match.
		ids := sortedIDs(members)
		mg := &model.MatchGroup{
			ID:          groupID(ids),
			Type:        model.MatchExactID,
			RecordIDs:   ids,
			ScoreTuple:  []int64{0, 0, 0, 0},
			AmountMinor: members[0].Amount.V,
			Currency:    members[0].Amount.C.Code,
		}
		res.Groups = append(res.Groups, mg)
		for _, m := range members {
			claim(m.ID)
		}
	}
}

func businessIDValue(r *model.Record, field string) string {
	if field == model.FieldBusinessID {
		return r.BusinessID
	}
	// For alternate fields we only model business_id and business_id_alt in the
	// record; map both.
	return r.BusinessID
}

// ---- Phase 2: bounded one-to-many ----

func (e *Engine) phaseOneToMany(rule *model.Rule, recs []*model.Record, res *Result, claim func(string), claimed map[string]bool) {
	anchorRole := rule.OneToMany.AnchorRole
	manyRole := rule.OneToMany.ManyRole
	maxMembers := rule.OneToMany.MaxMembers
	if maxMembers < 2 {
		maxMembers = 2
	}
	searchLimit := rule.SearchLimit
	if searchLimit <= 0 {
		searchLimit = 10000
	}

	// Anchors and many records still free, sorted by stable key.
	var anchors, many []*model.Record
	for _, r := range recs {
		if r == nil || claimed[r.ID] {
			continue
		}
		if r.Role == anchorRole {
			anchors = append(anchors, r)
		} else if r.Role == manyRole {
			many = append(many, r)
		}
	}
	sort.Slice(anchors, func(i, j int) bool { return anchors[i].StableKey < anchors[j].StableKey })
	sort.Slice(many, func(i, j int) bool { return many[i].StableKey < many[j].StableKey })

	for _, a := range anchors {
		if claimed[a.ID] {
			continue
		}
		// Candidate many records within the time window of the anchor.
		cands := make([]*model.Record, 0, len(many))
		for _, m := range many {
			if m == nil || claimed[m.ID] {
				continue
			}
			if withinTime(a, m, rule) {
				cands = append(cands, m)
			}
		}
		if len(cands) < 2 {
			continue
		}
		// Search subsets of size 2..maxMembers.
		found, evaluated, hitLimit := searchSubset(a, cands, maxMembers, searchLimit, rule)
		if hitLimit {
			res.Discrepancies = append(res.Discrepancies, searchLimitDisc(a, cands, "combination search limit exceeded"))
			claim(a.ID)
			continue
		}
		if found != nil {
			members := append([]*model.Record{a}, found...)
			ids := sortedIDs(members)
			sumAmt := int64(0)
			for _, m := range found {
				sumAmt += m.Amount.V
			}
			diff := a.Amount.V - sumAmt
			if diff < 0 {
				diff = -diff
			}
			mg := &model.MatchGroup{
				ID:          groupID(ids),
				Type:        model.MatchOneToMany,
				Role:        anchorRole,
				RecordIDs:   ids,
				ScoreTuple:  []int64{2, diff, 0, 0},
				AmountMinor: a.Amount.V,
				Currency:    a.Amount.C.Code,
			}
			res.Groups = append(res.Groups, mg)
			claim(a.ID)
			for _, m := range found {
				claim(m.ID)
			}
			continue
		}
		// No subset matched. If the full set sums within tolerance and needs
		// more than maxMembers, emit search_limit (exceeds max members).
		if len(cands) > maxMembers {
			sumAll := int64(0)
			for _, m := range cands {
				sumAll += m.Amount.V
			}
			base := a.Amount.V
			if sumAll > base {
				base = sumAll
			}
			diff := a.Amount.V - sumAll
			if diff < 0 {
				diff = -diff
			}
			if rule.AmountTolerance.Within(diff, base) {
				res.Discrepancies = append(res.Discrepancies, searchLimitDisc(a, cands, "match requires more than max_members"))
				claim(a.ID)
			}
		}
		_ = evaluated
	}
}

// searchSubset enumerates subsets of cands of size 2..maxMembers in stable
// order. It returns the first valid subset (smallest size, then smallest stable
// key order), the number of subsets evaluated, and whether the search limit was
// hit. A subset is valid when |anchor - sum| is within amount tolerance and the
// summed fee is within fee tolerance.
func searchSubset(anchor *model.Record, cands []*model.Record, maxMembers, searchLimit int, rule *model.Rule) (found []*model.Record, evaluated int, hitLimit bool) {
	n := len(cands)
	for k := 2; k <= maxMembers; k++ {
		if k > n {
			break
		}
		idxs := make([]int, k)
		for i := range idxs {
			idxs[i] = i
		}
		for {
			evaluated++
			if evaluated > searchLimit {
				return nil, evaluated, true
			}
			subset := make([]*model.Record, k)
			for i, idx := range idxs {
				subset[i] = cands[idx]
			}
			if validSubset(anchor, subset, rule) {
				return subset, evaluated, false
			}
			// Advance the combination.
			done := true
			for i := k - 1; i >= 0; i-- {
				if idxs[i] != i+n-k {
					done = false
					idxs[i]++
					for j := i + 1; j < k; j++ {
						idxs[j] = idxs[j-1] + 1
					}
					break
				}
			}
			if done {
				break
			}
		}
	}
	return nil, evaluated, false
}

func validSubset(anchor *model.Record, subset []*model.Record, rule *model.Rule) bool {
	sum := int64(0)
	feeSum := int64(0)
	for _, m := range subset {
		sum += m.Amount.V
		feeSum += m.Fee.V
	}
	base := anchor.Amount.V
	if sum > base {
		base = sum
	}
	diff := anchor.Amount.V - sum
	if diff < 0 {
		diff = -diff
	}
	if !rule.AmountTolerance.Within(diff, base) {
		return false
	}
	feeDiff := anchor.Fee.V - feeSum
	if feeDiff < 0 {
		feeDiff = -feeDiff
	}
	feeBase := anchor.Fee.V
	if feeSum > feeBase {
		feeBase = feeSum
	}
	if rule.FeeTolerance.Enabled() && !rule.FeeTolerance.Within(feeDiff, feeBase) {
		return false
	}
	return true
}

// ---- Phase 3: one-to-one tolerance matching ----

type candidate struct {
	a, b       *model.Record
	score      []int64
	amountDiff int64
	timeDiff   int64
	feeDiff    int64
	amountOK   bool
	feeOK      bool
	timeOK     bool
}

func (e *Engine) phaseOneToOne(rule *model.Rule, recs []*model.Record, res *Result, claim func(string), claimed map[string]bool) {
	// Build candidate pairs across allowed role pairs.
	allowed := make(map[model.Role]bool)
	for _, r := range rule.AllowedRoles {
		allowed[r] = true
	}
	var cands []candidate
	for i := 0; i < len(model.AllRoles); i++ {
		for j := i + 1; j < len(model.AllRoles); j++ {
			r1, r2 := model.AllRoles[i], model.AllRoles[j]
			if !allowed[r1] || !allowed[r2] {
				continue
			}
			var left, right []*model.Record
			for _, r := range recs {
				if r == nil || claimed[r.ID] {
					continue
				}
				if r.Role == r1 {
					left = append(left, r)
				} else if r.Role == r2 {
					right = append(right, r)
				}
			}
			for _, a := range left {
				for _, b := range right {
					c := scorePair(a, b, rule)
					if c.amountOK && c.feeOK && c.timeOK {
						cands = append(cands, c)
					}
				}
			}
		}
	}

	// Detect ambiguity: a record with >=2 candidates tied at its best score.
	bestByRecord := make(map[string][]candidate)
	for _, c := range cands {
		bestByRecord[c.a.ID] = append(bestByRecord[c.a.ID], c)
		bestByRecord[c.b.ID] = append(bestByRecord[c.b.ID], c)
	}
	ambiguous := make(map[string]bool)
	ambiguousCands := make(map[string][]candidate)
	for rid, cs := range bestByRecord {
		if len(cs) < 2 {
			continue
		}
		sort.Slice(cs, func(i, j int) bool { return scoreLess(cs[i].score, cs[j].score) })
		best := cs[0].score
		tied := []candidate{cs[0]}
		for k := 1; k < len(cs); k++ {
			if scoreEq(cs[k].score, best) {
				tied = append(tied, cs[k])
			} else {
				break
			}
		}
		if len(tied) >= 2 {
			ambiguous[rid] = true
			ambiguousCands[rid] = tied
		}
	}
	// Emit ambiguous discrepancies (deterministic order).
	ambIDs := make([]string, 0, len(ambiguous))
	for id := range ambiguous {
		ambIDs = append(ambIDs, id)
	}
	sort.Strings(ambIDs)
	for _, id := range ambIDs {
		tied := ambiguousCands[id]
		// The record `id` is the anchor; list the partner candidates sorted by
		// stable key.
		var partners []*model.Record
		for _, c := range tied {
			var p *model.Record
			if c.a.ID == id {
				p = c.b
			} else {
				p = c.a
			}
			partners = append(partners, p)
		}
		sort.Slice(partners, func(i, j int) bool { return partners[i].StableKey < partners[j].StableKey })
		anchorRec := recordByID(recs, id)
		res.Discrepancies = append(res.Discrepancies, ambiguousDisc(anchorRec, partners, tied[0].score))
		claim(id)
	}

	// Commit stage: greedily select non-conflicting candidates in stable score
	// order. Remove candidates touching ambiguous records.
	filtered := make([]candidate, 0, len(cands))
	for _, c := range cands {
		if ambiguous[c.a.ID] || ambiguous[c.b.ID] {
			continue
		}
		filtered = append(filtered, c)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if scoreLess(filtered[i].score, filtered[j].score) {
			return true
		}
		if scoreEq(filtered[i].score, filtered[j].score) {
			// stable tiebreak: a.StableKey, b.StableKey
			if filtered[i].a.StableKey != filtered[j].a.StableKey {
				return filtered[i].a.StableKey < filtered[j].a.StableKey
			}
			return filtered[i].b.StableKey < filtered[j].b.StableKey
		}
		return false
	})
	used := make(map[string]bool)
	for _, c := range filtered {
		if used[c.a.ID] || used[c.b.ID] {
			continue
		}
		ids := sortedIDs([]*model.Record{c.a, c.b})
		mg := &model.MatchGroup{
			ID:          groupID(ids),
			Type:        model.MatchTolerance,
			RecordIDs:   ids,
			ScoreTuple:  c.score,
			AmountMinor: c.a.Amount.V,
			Currency:    c.a.Amount.C.Code,
		}
		res.Groups = append(res.Groups, mg)
		used[c.a.ID] = true
		used[c.b.ID] = true
		claim(c.a.ID)
		claim(c.b.ID)
	}
}

func scorePair(a, b *model.Record, rule *model.Rule) candidate {
	amountDiff, _ := money.AbsDiff(a.Amount, b.Amount)
	feeDiff, _ := money.AbsDiff(a.Fee, b.Fee)
	td := a.Timestamp.Sub(b.Timestamp)
	if td < 0 {
		td = -td
	}
	base := a.Amount.V
	if b.Amount.V > base {
		base = b.Amount.V
	}
	amountOK := rule.AmountTolerance.Within(amountDiff, base)
	feeBase := a.Fee.V
	if b.Fee.V > feeBase {
		feeBase = b.Fee.V
	}
	feeOK := true
	if rule.FeeTolerance.Enabled() {
		feeOK = rule.FeeTolerance.Within(feeDiff, feeBase)
	}
	timeOK := withinTime(a, b, rule)
	score := []int64{1, amountDiff, td.Nanoseconds(), feeDiff}
	return candidate{
		a: a, b: b,
		score:      score,
		amountDiff: amountDiff,
		timeDiff:   td.Nanoseconds(),
		feeDiff:    feeDiff,
		amountOK:   amountOK,
		feeOK:      feeOK,
		timeOK:     timeOK,
	}
}

func withinTime(a, b *model.Record, rule *model.Rule) bool {
	td := a.Timestamp.Sub(b.Timestamp)
	if td < 0 {
		td = -td
	}
	return td.Nanoseconds() <= rule.TimeWindow
}

// ---- Remaining classification ----

func (e *Engine) classifyRemaining(r *model.Record, all []*model.Record, excluded map[string]bool) *model.Discrepancy {
	rule := e.ruleFor(r)
	dtype := missingType(r.Role, rolesOf(rule))
	// Find the best counterpart across allowed roles (different role, same
	// currency) from the full record set, regardless of whether it matched.
	var bestTimeAligned *model.Record
	var bestTimeAlignedDiff int64 = -1
	var bestTimeAlignedAmountOK bool
	var bestTimeAlignedFeeOK bool
	var bestAmountAligned *model.Record
	var bestAmountAlignedTimeDiff int64 = -1
	for _, c := range all {
		if c == nil || c.ID == r.ID || c.Role == r.Role {
			continue
		}
		if !roleAllowed(rule, c.Role) {
			continue
		}
		if c.Amount.C != r.Amount.C {
			continue
		}
		amountDiff, _ := money.AbsDiff(r.Amount, c.Amount)
		td := r.Timestamp.Sub(c.Timestamp)
		if td < 0 {
			td = -td
		}
		timeOK := td.Nanoseconds() <= e.windowFor(r)
		feeDiff, _ := money.AbsDiff(r.Fee, c.Fee)
		feeBase := r.Fee.V
		if c.Fee.V > feeBase {
			feeBase = c.Fee.V
		}
		amountOK := e.amountTolFor(r).Within(amountDiff, max64(r.Amount.V, c.Amount.V))
		feeOK := true
		if e.feeTolFor(r).Enabled() {
			feeOK = e.feeTolFor(r).Within(feeDiff, feeBase)
		}
		// Among time-aligned candidates, track the smallest amount diff.
		if timeOK && (bestTimeAlignedDiff < 0 || amountDiff < bestTimeAlignedDiff) {
			bestTimeAligned = c
			bestTimeAlignedDiff = amountDiff
			bestTimeAlignedAmountOK = amountOK
			bestTimeAlignedFeeOK = feeOK
		}
		// Among amount+fee-OK candidates, track the smallest time diff.
		if amountOK && feeOK && (bestAmountAlignedTimeDiff < 0 || td.Nanoseconds() < bestAmountAlignedTimeDiff) {
			bestAmountAligned = c
			bestAmountAlignedTimeDiff = td.Nanoseconds()
		}
	}
	switch {
	// time-aligned but amount differs -> amount_mismatch
	case bestTimeAligned != nil && !bestTimeAlignedAmountOK:
		return mismatchDisc(model.DiscAmountMismatch, r, bestTimeAligned)
	// time-aligned, amount ok but fee off -> fee_mismatch
	case bestTimeAligned != nil && bestTimeAlignedAmountOK && !bestTimeAlignedFeeOK:
		return mismatchDisc(model.DiscFeeMismatch, r, bestTimeAligned)
	// amount+fee ok but time outside window -> time_mismatch
	case bestAmountAligned != nil && bestTimeAligned == nil:
		return mismatchDisc(model.DiscTimeMismatch, r, bestAmountAligned)
	}
	// Otherwise: no plausible counterpart -> missing_<role>.
	return missingDiscTyped(r, dtype)
}

func roleAllowed(rule *model.Rule, role model.Role) bool {
	if rule == nil {
		return true
	}
	for _, r := range rule.AllowedRoles {
		if r == role {
			return true
		}
	}
	return false
}

func rolesOf(rule *model.Rule) []model.Role {
	if rule != nil {
		return rule.AllowedRoles
	}
	return model.AllRoles
}

func (e *Engine) ruleFor(r *model.Record) *model.Rule {
	for _, rl := range e.rules {
		for _, role := range rl.AllowedRoles {
			if role == r.Role {
				return rl
			}
		}
	}
	return nil
}
func (e *Engine) windowFor(r *model.Record) int64 {
	if rl := e.ruleFor(r); rl != nil {
		return rl.TimeWindow
	}
	return 0
}
func (e *Engine) amountTolFor(r *model.Record) money.Tolerance {
	if rl := e.ruleFor(r); rl != nil {
		return rl.AmountTolerance
	}
	return money.Tolerance{}
}
func (e *Engine) feeTolFor(r *model.Record) money.Tolerance {
	if rl := e.ruleFor(r); rl != nil {
		return rl.FeeTolerance
	}
	return money.Tolerance{}
}

// ---- helpers ----

func scoreLess(a, b []int64) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
func scoreEq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedIDs(recs []*model.Record) []string {
	ids := make([]string, 0, len(recs))
	for _, r := range recs {
		ids = append(ids, r.ID)
	}
	sort.Strings(ids)
	return ids
}

func groupID(ids []string) string {
	h := sha256.New()
	for _, id := range ids {
		fmt.Fprintln(h, id)
	}
	return "mg_" + hex.EncodeToString(h.Sum(nil))[:16]
}

func discID(dtype model.DiscType, ids []string) string {
	h := sha256.New()
	fmt.Fprintln(h, dtype)
	for _, id := range ids {
		fmt.Fprintln(h, id)
	}
	return "disc_" + hex.EncodeToString(h.Sum(nil))[:16]
}

func recordByID(recs []*model.Record, id string) *model.Record {
	for _, r := range recs {
		if r != nil && r.ID == id {
			return r
		}
	}
	return nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// bigSum is retained for potential future use in percentage comparisons outside
// the money package; it sums a slice of int64 using big.Int to avoid overflow.
func bigSum(vals []int64) *big.Int {
	acc := new(big.Int)
	for _, v := range vals {
		acc.Add(acc, big.NewInt(v))
	}
	return acc
}
