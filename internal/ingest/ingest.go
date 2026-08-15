// Package ingest implements the streaming import pipeline. Feeds arrive as CSV
// or NDJSON; rows are parsed and normalized concurrently by a bounded worker
// pool, but results are emitted in stable source-line order by an ordered
// merger. This makes the committed output identical for any worker count and
// any goroutine completion order: determinism is a property of the merger, not
// of the scheduler.
//
// The pipeline records a stable source_line number per row, attaches
// structured per-row validation errors, and supports backpressure via a bounded
// queue between the reader, the workers and the merger.
package ingest

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

// Format constants.
const (
	FormatCSV    = "csv"
	FormatNDJSON = "ndjson"
)

// RowResult is the normalized outcome for a single source line. Exactly one of
// Record or Error is non-nil.
type RowResult struct {
	Line   int64
	Record *model.Record
	Error  *RowError
	Raw    string
}

// RowError is a structured per-row validation failure.
type RowError struct {
	Line   int64  `json:"line"`
	Field  string `json:"field"`
	Code   string `json:"code"`
	Reason string `json:"reason"`
	Value  string `json:"value,omitempty"`
}

func (e *RowError) Error() string {
	return fmt.Sprintf("line %d field %s: %s (%s)", e.Line, e.Field, e.Reason, e.Code)
}

// Options configures the pipeline.
type Options struct {
	Workers   int           // size of the parse worker pool; must be >= 1.
	QueueSize int           // bounded queue depth between stages.
	Source    *model.Source // source configuration (field map, currency, tz).
	Registry  *money.Registry
}

// Pipeline reads a feed and yields RowResults in source-line order.
type Pipeline struct {
	opts Options
}

// New constructs a pipeline. Workers defaults to 1, QueueSize to 64.
func New(opts Options) *Pipeline {
	if opts.Workers < 1 {
		opts.Workers = 1
	}
	if opts.QueueSize < 1 {
		opts.QueueSize = 64
	}
	if opts.Registry == nil {
		opts.Registry = money.NewRegistry()
	}
	return &Pipeline{opts: opts}
}

// Run consumes the feed from r and invokes emit for each RowResult in stable
// source-line order. It blocks until the feed is exhausted or emit returns an
// error. The pipeline applies backpressure: it never reads more than
// QueueSize rows ahead of the merger.
func (p *Pipeline) Run(r io.Reader, emit func(RowResult) error) error {
	src := p.opts.Source
	if src == nil {
		return fmt.Errorf("ingest: nil source")
	}

	// rawLines: reader -> raw line channel (bounded).
	rawLines := make(chan rawLine, p.opts.QueueSize)
	// results: workers -> merger channel (bounded).
	results := make(chan RowResult, p.opts.QueueSize)

	var readerErr error
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		defer close(rawLines)
		readerErr = p.readLines(r, src.Format, rawLines)
	}()

	// Spawn workers.
	var workerWG sync.WaitGroup
	for i := 0; i < p.opts.Workers; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			p.worker(rawLines, results)
		}()
	}
	// Close results after all workers finish.
	go func() {
		workerWG.Wait()
		close(results)
	}()

	// Ordered merger: emit results in ascending line order regardless of
	// arrival order.
	if err := p.merge(results, emit); err != nil {
		return err
	}
	readerWG.Wait()
	if readerErr != nil {
		return readerErr
	}
	return nil
}

type rawLine struct {
	line int64
	text string
}

// readLines splits the input into logical lines and forwards them with a stable
// 1-based line number. CSV and NDJSON differ only in how a logical record is
// delimited; both are line-oriented here. For CSV the header is consumed and
// not counted as a record line.
func (p *Pipeline) readLines(r io.Reader, format string, out chan<- rawLine) error {
	br := bufio.NewReaderSize(r, 1<<16)
	var header []string
	var line int64
	switch format {
	case FormatCSV:
		// Read header line.
		h, err := readCSVRow(br)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		header = h
		for {
			row, err := readCSVRow(br)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			line++
			select {
			case out <- rawLine{line: line, text: encodeCSVRowWithHeader(header, row)}:
			}
		}
	case FormatNDJSON:
		for {
			b, err := br.ReadBytes('\n')
			if len(b) == 0 && err == io.EOF {
				return nil
			}
			text := strings.TrimRight(string(b), "\r\n ")
			if text == "" {
				if err == io.EOF {
					return nil
				}
				continue
			}
			line++
			select {
			case out <- rawLine{line: line, text: text}:
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("ingest: unknown format %q", format)
	}
}

func (p *Pipeline) worker(in <-chan rawLine, out chan<- RowResult) {
	for rl := range in {
		res := p.normalize(rl)
		out <- res
	}
}

// merge emits results in ascending line order. It buffers out-of-order results
// in a map keyed by line number and flushes contiguous lines as they arrive.
// Because line numbers are dense from 1..N, the buffer never holds more than
// the maximum out-of-order gap (bounded by the queue depth).
func (p *Pipeline) merge(in <-chan RowResult, emit func(RowResult) error) error {
	buffer := make(map[int64]RowResult)
	next := int64(1)
	for res := range in {
		if res.Line == next {
			if err := emit(res); err != nil {
				return err
			}
			next++
			for {
				if r, ok := buffer[next]; ok {
					delete(buffer, next)
					if err := emit(r); err != nil {
						return err
					}
					next++
				} else {
					break
				}
			}
		} else {
			buffer[res.Line] = res
		}
	}
	// Flush any remaining (should be empty if lines were dense).
	if len(buffer) > 0 {
		// Lines were not dense (shouldn't happen); emit in sorted order for
		// determinism.
		keys := make([]int64, 0, len(buffer))
		for k := range buffer {
			keys = append(keys, k)
		}
		sortInt64s(keys)
		for _, k := range keys {
			if err := emit(buffer[k]); err != nil {
				return err
			}
		}
	}
	return nil
}

func sortInt64s(a []int64) {
	// simple insertion sort; slices are small (bounded by queue depth).
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1] > a[j]; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}

// normalize parses one raw line into a RowResult.
func (p *Pipeline) normalize(rl rawLine) RowResult {
	src := p.opts.Source
	fields, perr := p.decodeLine(rl.text, src.Format)
	res := RowResult{Line: rl.line, Raw: rl.text}
	if perr != nil {
		res.Error = &RowError{Line: rl.line, Field: "_record", Code: "decode", Reason: perr.Error()}
		return res
	}
	rec := &model.Record{
		SourceID:   src.ID,
		Role:       src.Role,
		SourceLine: rl.line,
		RawLine:    rl.text,
	}
	// Required fields.
	externalID, ok := lookupField(fields, src.FieldMap, model.FieldExternalID)
	if !ok || externalID == "" {
		res.Error = &RowError{Line: rl.line, Field: model.FieldExternalID, Code: "missing", Reason: "external_id is required"}
		return res
	}
	rec.ExternalID = externalID

	amountRaw, ok := lookupField(fields, src.FieldMap, model.FieldAmount)
	if !ok || amountRaw == "" {
		res.Error = &RowError{Line: rl.line, Field: model.FieldAmount, Code: "missing", Reason: "amount is required"}
		return res
	}
	cur := p.currency(fields, src)
	if !IsKnown(cur) {
		res.Error = &RowError{Line: rl.line, Field: model.FieldCurrency, Code: "unknown_currency", Reason: "currency not registered: " + cur.Code, Value: cur.Code}
		return res
	}
	amt, aerr := money.Parse(cur, amountRaw, money.RoundReject)
	if aerr != nil {
		res.Error = &RowError{Line: rl.line, Field: model.FieldAmount, Code: aerr.Code, Reason: aerr.Reason, Value: amountRaw}
		return res
	}
	rec.Amount = amt

	// Optional fee.
	if feeRaw, ok := lookupField(fields, src.FieldMap, model.FieldFee); ok && feeRaw != "" {
		fee, ferr := money.Parse(cur, feeRaw, money.RoundReject)
		if ferr != nil {
			res.Error = &RowError{Line: rl.line, Field: model.FieldFee, Code: ferr.Code, Reason: ferr.Reason, Value: feeRaw}
			return res
		}
		rec.Fee = fee
	} else {
		rec.Fee = money.Zero(cur)
	}

	// Direction.
	if dir, ok := lookupField(fields, src.FieldMap, model.FieldDirection); ok && dir != "" {
		rec.Direction = strings.ToLower(strings.TrimSpace(dir))
		if rec.Direction != model.DirectionCredit && rec.Direction != model.DirectionDebit {
			res.Error = &RowError{Line: rl.line, Field: model.FieldDirection, Code: "invalid", Reason: "direction must be credit or debit", Value: dir}
			return res
		}
	}

	// Timestamp.
	tsRaw, ok := lookupField(fields, src.FieldMap, model.FieldTimestamp)
	if !ok || tsRaw == "" {
		res.Error = &RowError{Line: rl.line, Field: model.FieldTimestamp, Code: "missing", Reason: "timestamp is required"}
		return res
	}
	ts, terr := parseTimestamp(tsRaw, src.TimeZone)
	if terr != nil {
		res.Error = &RowError{Line: rl.line, Field: model.FieldTimestamp, Code: "invalid", Reason: terr.Error(), Value: tsRaw}
		return res
	}
	rec.Timestamp = ts.UTC()
	rec.TimestampRaw = tsRaw

	// Optional business id(s).
	if bid, ok := lookupField(fields, src.FieldMap, model.FieldBusinessID); ok {
		rec.BusinessID = strings.TrimSpace(bid)
	}
	if cp, ok := lookupField(fields, src.FieldMap, model.FieldCounterparty); ok {
		rec.Counterparty = strings.TrimSpace(cp)
	}
	if memo, ok := lookupField(fields, src.FieldMap, model.FieldMemo); ok {
		rec.Memo = strings.TrimSpace(memo)
	}

	rec.ContentHash = computeContentHash(rec)
	rec.ID = recordID(src, rec)
	rec.StableKey = stableKey(src, rec)
	res.Record = rec
	return res
}

func (p *Pipeline) currency(fields map[string]string, src *model.Source) money.Currency {
	code := src.Currency
	if cRaw, ok := lookupField(fields, src.FieldMap, model.FieldCurrency); ok && cRaw != "" {
		code = cRaw
	}
	if c, ok := p.opts.Registry.Lookup(code); ok {
		return c
	}
	// Unknown currency: return a placeholder with the requested code and a
	// sentinel scale of 255 so the caller can detect it via IsKnown.
	return money.Currency{Code: strings.ToUpper(code), Scale: money.UnknownScale}
}

// IsKnown reports whether c is a registered currency (not the unknown
// placeholder).
func IsKnown(c money.Currency) bool { return c.Scale != money.UnknownScale }

// lookupField resolves a logical field name to its raw value via the source
// field map. If the field map maps the logical name to a raw column, that
// column is read; otherwise the logical name itself is used as the column.
func lookupField(fields map[string]string, fieldMap map[string]string, logical string) (string, bool) {
	col := logical
	if c, ok := fieldMap[logical]; ok && c != "" {
		col = c
	}
	v, ok := fields[col]
	return v, ok
}

// decodeLine parses a raw line into a field map according to the format.
// For CSV lines produced by readLines, the text is a JSON-encoded [header,row]
// pair so we can reconstruct column names. For NDJSON, the text is a JSON
// object.
func (p *Pipeline) decodeLine(text string, format string) (map[string]string, error) {
	switch format {
	case FormatCSV:
		var pair []interface{}
		if err := json.Unmarshal([]byte(text), &pair); err != nil {
			return nil, fmt.Errorf("csv decode: %w", err)
		}
		if len(pair) != 2 {
			return nil, fmt.Errorf("csv decode: expected [header,row]")
		}
		hdrArr, _ := pair[0].([]interface{})
		rowArr, _ := pair[1].([]interface{})
		out := make(map[string]string, len(hdrArr))
		for i, h := range hdrArr {
			key := fmt.Sprintf("%v", h)
			if i < len(rowArr) {
				if s, ok := rowArr[i].(string); ok {
					out[key] = s
				} else if rowArr[i] != nil {
					out[key] = fmt.Sprintf("%v", rowArr[i])
				}
			}
		}
		return out, nil
	case FormatNDJSON:
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(text), &obj); err != nil {
			return nil, fmt.Errorf("ndjson decode: %w", err)
		}
		out := make(map[string]string, len(obj))
		for k, v := range obj {
			if v == nil {
				continue
			}
			switch t := v.(type) {
			case string:
				out[k] = t
			case float64:
				// Render without float drift using JSON marshal of the number.
				b, _ := json.Marshal(t)
				out[k] = string(b)
			case bool:
				out[k] = strconv.FormatBool(t)
			default:
				b, _ := json.Marshal(t)
				out[k] = string(b)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown format %q", format)
	}
}

func parseTimestamp(raw, tzName string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	// Try a series of layouts.
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02",
	}
	loc := time.UTC
	if tzName != "" {
		if l, err := time.LoadLocation(tzName); err == nil {
			loc = l
		}
	}
	var firstErr error
	for _, lay := range layouts {
		// If the string carries an explicit zone offset, parse in UTC and let
		// the offset apply; otherwise parse in the source timezone.
		if hasOffset(raw) {
			if t, err := time.Parse(lay, raw); err == nil {
				return t, nil
			}
		} else {
			if t, err := time.ParseInLocation(lay, raw, loc); err == nil {
				return t, nil
			} else if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		return time.Time{}, firstErr
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp")
}

func hasOffset(s string) bool {
	// crude: presence of 'Z' or '+HH:MM' after a 'T' or space.
	if strings.ContainsAny(s, "Zz") {
		return true
	}
	// look for + or - after position 10.
	if len(s) > 10 {
		rest := s[10:]
		return strings.ContainsAny(rest, "+-")
	}
	return false
}

func computeContentHash(r *model.Record) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%d|%s|%d|%s|%s", r.Role, r.Amount.C.Code, r.Amount.V, r.Direction, r.Timestamp.UnixNano(), r.Counterparty, r.BusinessID)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// recordID is deterministic: hash of source + source_line + content.
func recordID(src *model.Source, r *model.Record) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%d|%s", src.ID, r.SourceLine, r.ContentHash)
	return "rec_" + hex.EncodeToString(h.Sum(nil))[:16]
}

func stableKey(src *model.Source, r *model.Record) string {
	// Stable across reruns: role + external_id + content hash. External id is
	// unique within a source; content hash disambiguates re-ordered files.
	return fmt.Sprintf("%s|%s|%s", r.Role, r.ExternalID, r.ContentHash)
}

// encodeCSVRowWithHeader packs the header and a row into a JSON array string so
// the worker can reconstruct named fields without re-reading the header.
func encodeCSVRowWithHeader(header, row []string) string {
	pair := [2]interface{}{toAny(header), toAny(row)}
	b, _ := json.Marshal(pair)
	return string(b)
}

func toAny(s []string) []interface{} {
	out := make([]interface{}, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
