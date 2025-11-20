package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/O-Nicolinho/sixpaths_kv/internal/sixpaths_kvs"
)

// router/main.go is the front-end router for our cluster
// to allow 3rd parties to interact with our kv store.
// we handle /put, /delete, /get, /metrics.
// for writes and reads, we hash it in order to
// make sure that the right command is sent to the right node.

// the router is a stateless front-end that works as an interface for interactions with the our KV cluster
type router struct {
	nodes       []sixpaths_kvs.NodeConfig // list of backend nodes in the cluster
	backendHost string                    // the host where we can actually reach the nodes
}

type nodeMetrics struct {
	ID      string                       `json:"id"`
	Addr    string                       `json:"addr"`
	Metrics sixpaths_kvs.MetricsSnapshot `json:"metrics"`
}

func main() {
	// the addr is where the router listens for client traffic
	addr := flag.String("addr", ":8080", "router listen address")

	//the backendhost is the host we use to talk to backend nodes
	// the ports of the nodes are in NodeConfig.Clientaddr
	backendHost := flag.String("backend-host", "127.0.0.1", "host for backend nodes")
	flag.Parse()

	// we load the static cluster config (which includes IDs, client ports, datadirs, etc)
	nodes := sixpaths_kvs.ClusterConfig()
	if len(nodes) == 0 {
		log.Fatalf("no nodes in cluster config")
	}

	// we build the router with the cluster nodes and backend host we got
	r := &router{
		nodes:       nodes,
		backendHost: *backendHost,
	}

	// we only expose put and get on the router
	mux := http.NewServeMux()
	mux.HandleFunc("/put", r.handlePut)
	mux.HandleFunc("/get", r.handleGet)
	mux.HandleFunc("/metrics", r.handleMetrics)
	mux.HandleFunc("/delete", r.handleDelete)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// start server!
	log.Printf("router listening at %s, managing %d nodes", *addr, len(nodes))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("router server error: %v", err)
	}
}

// ===== helpers =====

// as the name implies, this chooses which node will hold a given key.
// it currently works by hashing a key and mods by the num of nodes, giving a simplemapping
func (r *router) pickNodeForKey(key string) sixpaths_kvs.NodeConfig {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := int(h.Sum32()) % len(r.nodes)
	if idx < 0 {
		idx = -idx
	}
	return r.nodes[idx]
}

func proxyError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
	})
}

// ===== handlers =====

// POST /put
// same JSON as node: { "client": "...", "seq": 1, "key": "a", "value": "v1" }

// the router routes a PUT from a client to the correct backend node,
// it reads and parses the body to extract the key and
// forwards the info to the node, while streaming the node's response
func (r *router) handlePut(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		proxyError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Read whole body once so we can both inspect it and forward it.
	body, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil {
		proxyError(w, http.StatusBadRequest, "unable to read body")
		return
	}
	_ = req.Body.Close()

	var parsed struct {
		Client string `json:"client"`
		Seq    uint64 `json:"seq"`
		Key    string `json:"key"`
		Value  string `json:"value"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		proxyError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if parsed.Key == "" {
		proxyError(w, http.StatusBadRequest, "missing key")
		return
	}

	// we choose node the node according to our pickNodeForKey funct

	node := r.pickNodeForKey(parsed.Key)

	log.Printf("ROUTER: PUT key=%q client=%s seq=%d -> node=%s addr=%s",
		parsed.Key, parsed.Client, parsed.Seq, node.ID, node.ClientAddr)

	backendURL := fmt.Sprintf("http://%s%s/put", r.backendHost, node.ClientAddr)
	resp, err := http.Post(backendURL, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("proxy PUT to %s failed: %v", backendURL, err)
		proxyError(w, http.StatusBadGateway, "backend unavailable")
		return
	}
	defer resp.Body.Close()

	// Forward status code and body as-is.
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// GET /get?key=...

// handleGet routes a client's GET to the correct node based on the key
func (r *router) handleGet(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		proxyError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	key := req.URL.Query().Get("key")
	if key == "" {
		proxyError(w, http.StatusBadRequest, "missing key")
		return
	}

	// we pick the backend node we're get'ing from based on the hashed key
	node := r.pickNodeForKey(key)

	log.Printf("ROUTER: GET key=%q -> node=%s addr=%s", key, node.ID, node.ClientAddr)

	q := url.Values{}
	q.Set("key", key)
	backendURL := fmt.Sprintf("http://%s%s/get?%s", r.backendHost, node.ClientAddr, q.Encode())

	resp, err := http.Get(backendURL)
	if err != nil {
		log.Printf("proxy GET to %s failed: %v", backendURL, err)
		proxyError(w, http.StatusBadGateway, "backend unavailable")
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// we get metrics for each of the nodes and create a new cluster wide metrics report
func (r *router) handleMetrics(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		proxyError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// simple HTTP client with a small timeout so a dead node doesn't block everything
	client := &http.Client{
		Timeout: 500 * time.Millisecond,
	}

	var out []nodeMetrics

	for _, n := range r.nodes {
		url := fmt.Sprintf("http://%s%s/metrics", r.backendHost, n.ClientAddr)

		resp, err := client.Get(url)
		if err != nil {
			log.Printf("router: metrics fetch failed for node=%s addr=%s: %v", n.ID, n.ClientAddr, err)
			// still include an entry, but mark metrics as zero-values
			out = append(out, nodeMetrics{
				ID:      n.ID,
				Addr:    n.ClientAddr,
				Metrics: sixpaths_kvs.MetricsSnapshot{},
			})
			continue
		}

		var snap sixpaths_kvs.MetricsSnapshot
		if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
			log.Printf("router: metrics decode failed for node=%s addr=%s: %v", n.ID, n.ClientAddr, err)
			_ = resp.Body.Close()
			out = append(out, nodeMetrics{
				ID:      n.ID,
				Addr:    n.ClientAddr,
				Metrics: sixpaths_kvs.MetricsSnapshot{},
			})
			continue
		}
		_ = resp.Body.Close()

		out = append(out, nodeMetrics{
			ID:      n.ID,
			Addr:    n.ClientAddr,
			Metrics: snap,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(out)
}

// routes a client's DELETE intruct to the correct backend node
func (r *router) handleDelete(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		proxyError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Read body so we can both inspect and forward it.
	body, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil {
		proxyError(w, http.StatusBadRequest, "unable to read body")
		return
	}
	_ = req.Body.Close()

	// Minimal struct to pull out the key (and validate client/seq).
	var parsed struct {
		Client string `json:"client"`
		Seq    uint64 `json:"seq"`
		Key    string `json:"key"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		proxyError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if parsed.Client == "" || parsed.Seq == 0 || parsed.Key == "" {
		proxyError(w, http.StatusBadRequest, "missing client/seq/key")
		return
	}

	// Pick backend node based on hashed key.
	node := r.pickNodeForKey(parsed.Key)

	// Log which node this delete is going to.
	log.Printf("ROUTER: DELETE key=%q client=%s seq=%d -> node=%s addr=%s",
		parsed.Key, parsed.Client, parsed.Seq, node.ID, node.ClientAddr)

	backendURL := fmt.Sprintf("http://%s%s/delete", r.backendHost, node.ClientAddr)

	// Forward the request body to the backend node as POST /delete.
	resp, err := http.Post(backendURL, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("proxy DELETE to %s failed: %v", backendURL, err)
		proxyError(w, http.StatusBadGateway, "backend unavailable")
		return
	}
	defer resp.Body.Close()

	// Forward backend response status + body to the client.
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
