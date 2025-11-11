package sixpaths_kvs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// Models

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

// Server

// Server

type HTTPServer struct {
	node *Node
	addr string
	mux  *http.ServeMux
	srv  *http.Server
}

func NewHTTPServer(node *Node, addr string) *HTTPServer {
	mux := http.NewServeMux()
	h := &HTTPServer{
		node: node,
		addr: addr,
		mux:  mux,
	}
	mux.HandleFunc("/put", h.handlePut)
	mux.HandleFunc("/del", h.handleDel)
	mux.HandleFunc("/get", h.handleGet)
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/metrics", h.handleMetrics)

	h.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return h
}

func (h *HTTPServer) Start() error {
	return h.srv.ListenAndServe()
}

func (h *HTTPServer) Shutdown(ctx context.Context) error {
	return h.srv.Shutdown(ctx)
}

// Handlers

func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var req putReq
	if err := decodeJSON(w, r, &req, 1<<20); err != nil { // 1MB limit
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Client == "" || req.Seq == 0 || req.Key == "" {
		writeError(w, http.StatusBadRequest, "missing client/seq/key")
		return
	}
	cmd := Command{
		Instruct: CmdPut,
		ClientID: req.Client,
		Seq:      req.Seq,
		Key:      []byte(req.Key),
		Value:    []byte(req.Value),
	}
	res, err := h.node.Exec(cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	IncExec()
	IncPut()
	writeJSON(w, http.StatusOK, putDelResp{
		Success:   res.Success,
		PrevValue: string(res.PrevValue),
		LogIndex:  res.LogIndex,
	})
}

func (h *HTTPServer) handleDel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var req delReq
	if err := decodeJSON(w, r, &req, 1<<20); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Client == "" || req.Seq == 0 || req.Key == "" {
		writeError(w, http.StatusBadRequest, "missing client/seq/key")
		return
	}
	cmd := Command{
		Instruct: CmdDelete,
		ClientID: req.Client,
		Seq:      req.Seq,
		Key:      []byte(req.Key),
	}
	res, err := h.node.Exec(cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	IncExec()
	IncDel()
	writeJSON(w, http.StatusOK, putDelResp{
		Success:   res.Success,
		PrevValue: string(res.PrevValue),
		LogIndex:  res.LogIndex,
	})
}

func (h *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
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
	// ensure no trailing junk
	if dec.More() {
		return errors.New("unexpected extra JSON content")
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
