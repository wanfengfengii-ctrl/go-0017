// Package api exposes the SettleMesh service over a small REST API. Endpoints:
//
//	POST /sources                       create a source
//	PUT  /rulesets                      put a rule set
//	POST /batches                       submit+commit an import batch
//	GET  /batches                       list batches
//	POST /runs                          start a reconciliation run
//	GET  /runs                          list runs
//	GET  /runs/{id}                     get a run
//	POST /runs/{id}/cancel              cancel a run
//	GET  /runs/{id}/report?format=json  export a report
//
// All write endpoints accept an Idempotency-Key header. Responses are JSON.
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/service"
)

// Server is the HTTP API.
type Server struct {
	svc *service.Service
	mux *http.ServeMux
}

// New constructs a Server.
func New(svc *service.Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /sources", s.handlePostSource)
	s.mux.HandleFunc("PUT /rulesets", s.handlePutRuleset)
	s.mux.HandleFunc("POST /batches", s.handlePostBatch)
	s.mux.HandleFunc("GET /batches", s.handleListBatches)
	s.mux.HandleFunc("POST /runs", s.handlePostRun)
	s.mux.HandleFunc("GET /runs", s.handleListRuns)
	s.mux.HandleFunc("GET /runs/{id}", s.handleGetRun)
	s.mux.HandleFunc("POST /runs/{id}/cancel", s.handleCancelRun)
	s.mux.HandleFunc("GET /runs/{id}/report", s.handleGetReport)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handlePostSource(w http.ResponseWriter, r *http.Request) {
	var src model.Source
	if err := json.NewDecoder(r.Body).Decode(&src); err != nil {
		writeError(w, 400, "invalid source: "+err.Error())
		return
	}
	if err := s.svc.AddSource(&src); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, 201, src)
}

func (s *Server) handlePutRuleset(w http.ResponseWriter, r *http.Request) {
	var rs model.RuleSet
	if err := json.NewDecoder(r.Body).Decode(&rs); err != nil {
		writeError(w, 400, "invalid ruleset: "+err.Error())
		return
	}
	if err := s.svc.PutRuleSet(&rs); err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, rs)
}

type batchRequest struct {
	SourceID       string `json:"source_id"`
	Format         string `json:"format"`
	RowPolicy      string `json:"row_policy"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (s *Server) handlePostBatch(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	// Split a JSON envelope from the raw feed: the request body is the raw feed
	// and the parameters come from headers/query. For the API we expect a
	// multipart-style: source_id and format as query params, body = feed.
	sourceID := r.URL.Query().Get("source_id")
	format := r.URL.Query().Get("format")
	policyStr := r.URL.Query().Get("row_policy")
	policy := model.RowPolicyIsolate
	if policyStr == "atomic" {
		policy = model.RowPolicyAtomic
	}
	if key == "" {
		key = r.URL.Query().Get("idempotency_key")
	}
	b, err := s.svc.SubmitAndCommit(sourceID, key, format, strings.NewReader(string(body)), policy)
	if err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, 201, b)
}

func (s *Server) handleListBatches(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.svc.ListBatches())
}

type runRequest struct {
	RunID        string   `json:"run_id"`
	BatchIDs     []string `json:"batch_ids"`
	RuleRevision int64    `json:"rule_revision"`
	Workers      int      `json:"workers"`
}

func (s *Server) handlePostRun(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	run, err := s.svc.StartRun(req.RunID, req.BatchIDs, req.RuleRevision, req.Workers)
	if err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, 201, run)
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.svc.ListRuns())
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.svc.GetRun(id)
	if err != nil {
		writeError(w, 404, err.Error())
		return
	}
	writeJSON(w, 200, run)
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.svc.CancelRun(id)
	if err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, 200, run)
}

func (s *Server) handleGetReport(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	format := r.URL.Query().Get("format")
	data, err := s.svc.ExportReport(id, format)
	if err != nil {
		writeError(w, 404, err.Error())
		return
	}
	ct := "application/json"
	if format == "csv" {
		ct = "text/csv"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(200)
	_, _ = w.Write(data)
}

// Listen starts an HTTP server on addr. It blocks until the server stops.
func (s *Server) Listen(addr string) error {
	srv := &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

var _ = fmt.Sprintf
