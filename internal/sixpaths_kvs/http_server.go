package sixpaths_kvs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"
)

// ===== Models =====

type putReq struct {
	Client string `json:"client"`
	Seq    uint64 `json:"seq"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

type delReq struct {
	Client string `json:"client"`
	Seq    uint64 `json:"seq"`
	Key    string `json:"key"`
}

type putDelResp struct {
	Success   bool   `json:"success"`
	PrevValue string `json:"prevValue"`
	LogIndex  uint64 `json:"logIndex"`
}

type getResp struct {
	Value string `json:"value"`
}

type errResp struct {
	Error string `json:"error"`
}

type healthResp struct {
	Status    string `json:"status"`
	LastIndex uint64 `json:"lastIndex"`
}

// =====Server =====

type HTTPServer struct {
	node *Node
	addr string         //Listen address
	mux  *http.ServeMux // router (routes paths to handlers)
	srv  *http.Server   // (our http server)
}

// constructs the http server, registers routes, and prepares an http.Server with
// reasonable timeouts.
func NewHTTPServer(node *Node, addr string) *HTTPServer {
	mux := http.NewServeMux()
	h := &HTTPServer{
		node: node,
		addr: addr,
		mux:  mux,
	}

	// each path maps to a handler method
	// handlers are responsible for method checking
	mux.HandleFunc("/put", h.handlePut)
	mux.HandleFunc("/del", h.handleDel)
	mux.HandleFunc("/get", h.handleGet)
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/metrics", h.handleMetrics)

	// we set timeouts we deem appropriate
	h.srv = &http.Server{
		Addr:              addr,
		Handler:           h.withLogging(mux),
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return h
}

// starts listening and serving http requests on h.addr
func (h *HTTPServer) Start() error {
	return h.srv.ListenAndServe()
}

// shuts the server down gracefully, so we stop accepting new conns and we
// wait for ongoing requests to complete.
func (h *HTTPServer) Shutdown(ctx context.Context) error {
	return h.srv.Shutdown(ctx)
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (sr *statusRecorder) WriteHeader(code int) {
	if !sr.wroteHeader {
		sr.status = code
		sr.wroteHeader = true
	}
	sr.ResponseWriter.WriteHeader(code)
}

func (sr *statusRecorder) Write(p []byte) (int, error) {
	// If the handler didn’t call WriteHeader, default to 200 on first write.
	if !sr.wroteHeader {
		sr.WriteHeader(http.StatusOK)
	}
	n, err := sr.ResponseWriter.Write(p)
	sr.bytes += n
	return n, err
}

func (h *HTTPServer) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sr := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK, // default if never set explicitly
		}

		next.ServeHTTP(sr, r)

		dur := time.Since(start)

		log.Printf("method=%s path=%s status=%d bytes=%d dur=%s remote=%s",
			r.Method, r.URL.Path, sr.status, sr.bytes, dur, r.RemoteAddr)
	})
}

// ==== Handlers =====

// POST /put
// Body: {"client": "...", "Seq": N, "key": "K", "value": "V"}
func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	// we decode the JSON body into a putReq
	var req putReq
	if err := decodeJSON(w, r, &req, 1<<20); err != nil { // 1MB limit
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// validate the inputs
	if req.Client == "" || req.Seq == 0 || req.Key == "" {
		writeError(w, http.StatusBadRequest, "missing client/seq/key")
		return
	}

	// now we map the json request to our Command struct
	cmd := Command{
		Instruct: CmdPut,
		ClientID: req.Client,
		Seq:      req.Seq,
		Key:      []byte(req.Key),
		Value:    []byte(req.Value),
	}
	// now we execute the command via our node
	res, err := h.node.Exec(cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// we increase metric counters
	IncExec()
	IncPut()

	// build and send json response
	writeJSON(w, http.StatusOK, putDelResp{
		Success:   res.Success,
		PrevValue: string(res.PrevValue),
		LogIndex:  res.LogIndex,
	})
}

// POST /del
// Body: {"client": "...", "seq": N, "key: "K"}
func (h *HTTPServer) handleDel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	// we decode the json to build our delReq
	var req delReq
	if err := decodeJSON(w, r, &req, 1<<20); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// input validation
	if req.Client == "" || req.Seq == 0 || req.Key == "" {
		writeError(w, http.StatusBadRequest, "missing client/seq/key")
		return
	}
	// we map the json request to our Command struct
	cmd := Command{
		Instruct: CmdDelete,
		ClientID: req.Client,
		Seq:      req.Seq,
		Key:      []byte(req.Key),
	}
	// we execute the command
	res, err := h.node.Exec(cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// increase metrics
	IncExec()
	IncDel()
	// build and send json
	writeJSON(w, http.StatusOK, putDelResp{
		Success:   res.Success,
		PrevValue: string(res.PrevValue),
		LogIndex:  res.LogIndex,
	})
}

// GET /get?key=K
func (h *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	// read query key from URL
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}
	val, err := h.node.Get(key)
	if err != nil {
		// If your Store.Get returns a specific not-found error, map to 404
		// Otherwise default to 404 on any error here.
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, getResp{Value: string(val)})
}

// GET /health
// checks health / readiness
func (h *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, healthResp{
		Status:    "ok",
		LastIndex: h.node.last,
	})
}

// GET /metrics
// returns a snapshot of our metrics
func (h *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, Snapshot())
}

// Helpers

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	defer r.Body.Close()

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return err
	}

	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("unexpected extra JSON content")
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errResp{Error: msg})
}

func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}
