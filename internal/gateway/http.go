package gateway

// The HTTP surface. Three kinds of endpoint: one that streams the cluster
// view, ones that run key-value operations through it, and ones that break
// it on purpose.
//
// Everything here assumes the internet is on the other side. The cluster
// this fronts is a public demo, so the limits below are not decoration:
// they are what keeps a store anybody can write to from becoming a store
// anybody can fill.

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sujalbistaa/DistKV/internal/transport"
)

// Options configures a Server.
type Options struct {
	// WebDir is a directory of static files to serve at /. Empty serves
	// the API alone.
	WebDir string

	// AllowChaos enables the fault-injection endpoints. Turning it off
	// leaves a read-only console, which is what a cluster doing anything
	// real should be fronted by.
	AllowChaos bool

	// MaxKeyLen, MaxValueLen, and MaxKeys bound what visitors can store.
	MaxKeyLen   int
	MaxValueLen int
	MaxKeys     int

	// WritesPerMinute and FaultsPerMinute are per-IP budgets. Reads are
	// unlimited: they cost a log entry each, but so does everything in a
	// linearizable store, and rate-limiting them would hide the fact.
	WritesPerMinute int
	FaultsPerMinute int

	// TrustProxyHeaders makes the rate limiter read the client address
	// from X-Forwarded-For. Set it only when something in front of this
	// process is guaranteed to set that header, since a client can
	// otherwise forge it and get a fresh budget per request.
	TrustProxyHeaders bool
}

// DefaultOptions are tuned for a public demo: generous enough that nobody
// notices the limits while poking at it, tight enough that nobody can
// leave it unusable for the next visitor.
func DefaultOptions() Options {
	return Options{
		AllowChaos:      true,
		MaxKeyLen:       64,
		MaxValueLen:     512,
		MaxKeys:         200,
		WritesPerMinute: 60,
		FaultsPerMinute: 30,
	}
}

// Server is the gateway's HTTP handler.
type Server struct {
	cluster *Cluster
	chaos   *Chaos
	client  *Client
	opts    Options

	writeLimit *limiter
	faultLimit *limiter

	// keys tracks what has been stored through this gateway, which is how
	// MaxKeys is enforced. It is a cap on this demo's front door, not on
	// the store, which neither knows nor cares that it exists.
	mu   sync.Mutex
	keys map[string]struct{}
}

// NewServer wires a cluster, its fault injection, and a client into one
// HTTP handler.
func NewServer(cluster *Cluster, chaos *Chaos, client *Client, opts Options) *Server {
	return &Server{
		cluster:    cluster,
		chaos:      chaos,
		client:     client,
		opts:       opts,
		writeLimit: newLimiter(opts.WritesPerMinute),
		faultLimit: newLimiter(opts.FaultsPerMinute),
		keys:       make(map[string]struct{}),
	}
}

// Handler returns the routed handler for the whole gateway.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/cluster", s.handleCluster)
	mux.HandleFunc("GET /api/stream", s.handleStream)
	mux.HandleFunc("GET /api/config", s.handleConfig)
	mux.HandleFunc("GET /api/keys", s.handleKeys)
	mux.HandleFunc("POST /api/kv/{op}", s.handleKV)
	mux.HandleFunc("POST /api/chaos/{action}", s.handleChaos)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	if s.opts.WebDir != "" {
		mux.Handle("GET /", spaHandler(s.opts.WebDir))
	}

	return withCORS(mux)
}

// --- cluster view ---

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": s.cluster.Snapshot(),
		"events":   s.cluster.EventsSince(0),
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"allowChaos":  s.opts.AllowChaos,
		"maxKeyLen":   s.opts.MaxKeyLen,
		"maxValueLen": s.opts.MaxValueLen,
		"maxKeys":     s.opts.MaxKeys,
		"quorum":      s.cluster.Quorum(),
		"size":        len(s.cluster.Members()),
	})
}

// handleStream pushes the cluster view to the browser over SSE: a
// `snapshot` event whenever the poller sees anything, and `events` for the
// log. Server-sent events rather than a websocket because the traffic is
// one-directional and this survives any proxy that can handle plain HTTP.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Proxies that buffer would defeat the entire point of a live view.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	updates, unsubscribe := s.cluster.Subscribe()
	defer unsubscribe()

	var lastEvent uint64
	send := func() bool {
		if err := writeSSE(w, "snapshot", s.cluster.Snapshot()); err != nil {
			return false
		}
		if events := s.cluster.EventsSince(lastEvent); len(events) > 0 {
			lastEvent = events[len(events)-1].Seq
			if err := writeSSE(w, "events", events); err != nil {
				return false
			}
		}
		flusher.Flush()
		return true
	}

	if !send() {
		return
	}

	// A heartbeat keeps intermediaries from reaping a connection that has
	// nothing to say, which happens whenever the cluster is idle and
	// healthy — the state it spends most of its life in.
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-updates:
			if !send() {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// --- key-value operations ---

type kvRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.keys))
	for k := range s.keys {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys, "max": s.opts.MaxKeys})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	op := r.PathValue("op")
	switch op {
	case "get", "put", "append", "delete":
	default:
		writeError(w, http.StatusNotFound, fmt.Sprintf("unknown operation %q", op))
		return
	}

	var req kvRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.validate(op, req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if op != "get" && !s.writeLimit.allow(clientIP(r, s.opts.TrustProxyHeaders)) {
		writeError(w, http.StatusTooManyRequests,
			fmt.Sprintf("this demo allows %d writes per minute per visitor; reads are unlimited", s.opts.WritesPerMinute))
		return
	}

	result := s.client.Do(r.Context(), op, req.Key, req.Value)
	s.trackKey(op, req.Key, result.OK)
	s.recordOperation(op, req.Key, result)
	writeJSON(w, http.StatusOK, result)
}

// validate enforces the shape of what a visitor may store. The key cap is
// checked against keys this gateway has written, so a rejection can name a
// real number rather than guessing at the store's size.
func (s *Server) validate(op string, req kvRequest) error {
	if req.Key == "" {
		return fmt.Errorf("a key is required")
	}
	if len(req.Key) > s.opts.MaxKeyLen {
		return fmt.Errorf("keys are limited to %d characters here", s.opts.MaxKeyLen)
	}
	if strings.ContainsAny(req.Key, "\n\r") {
		return fmt.Errorf("keys may not contain newlines")
	}
	if op == "get" || op == "delete" {
		return nil
	}
	if len(req.Value) > s.opts.MaxValueLen {
		return fmt.Errorf("values are limited to %d characters here", s.opts.MaxValueLen)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.keys[req.Key]; !exists && len(s.keys) >= s.opts.MaxKeys {
		return fmt.Errorf("this demo holds at most %d keys; delete one to store another", s.opts.MaxKeys)
	}
	return nil
}

func (s *Server) trackKey(op, key string, ok bool) {
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op {
	case "put", "append":
		s.keys[key] = struct{}{}
	case "delete":
		delete(s.keys, key)
	}
}

// recordOperation puts successful writes and interesting failures into the
// same event log as the cluster's own transitions, so the console reads as
// one story: a write, the election that interrupted it, the retry that
// succeeded.
func (s *Server) recordOperation(op, key string, result Result) {
	switch {
	case result.OK && op == "get":
		return // reads are constant traffic; logging them would bury everything else
	case result.OK:
		detail := ""
		if len(result.Attempts) > 1 {
			detail = fmt.Sprintf(" after %d hops", len(result.Attempts))
		}
		s.cluster.Record("write", result.ServedBy, fmt.Sprintf(
			"%s %q committed at log index %d by %s%s", op, key, result.Index, result.ServedBy, detail))
	default:
		s.cluster.Record("write", "", fmt.Sprintf("%s %q failed: %s", op, key, result.Error))
	}
}

// --- fault injection ---

type chaosRequest struct {
	Node  string   `json:"node"`
	Group []string `json:"group"`
}

func (s *Server) handleChaos(w http.ResponseWriter, r *http.Request) {
	if !s.opts.AllowChaos {
		writeError(w, http.StatusForbidden, "fault injection is disabled on this deployment")
		return
	}
	if !s.faultLimit.allow(clientIP(r, s.opts.TrustProxyHeaders)) {
		writeError(w, http.StatusTooManyRequests,
			fmt.Sprintf("this demo allows %d faults per minute per visitor", s.opts.FaultsPerMinute))
		return
	}

	var req chaosRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	action := r.PathValue("action")
	var err error
	var affected string

	switch action {
	case "crash":
		affected = req.Node
		err = s.chaos.Crash(r.Context(), transport.NodeID(req.Node))
	case "crash-leader":
		var id transport.NodeID
		id, err = s.chaos.CrashLeader(r.Context())
		affected = string(id)
	case "restart":
		affected = req.Node
		err = s.chaos.Restart(r.Context(), transport.NodeID(req.Node))
	case "isolate":
		affected = req.Node
		err = s.chaos.Partition(r.Context(), []transport.NodeID{transport.NodeID(req.Node)})
	case "partition":
		group := make([]transport.NodeID, 0, len(req.Group))
		for _, id := range req.Group {
			group = append(group, transport.NodeID(id))
		}
		affected = strings.Join(req.Group, ", ")
		err = s.chaos.Partition(r.Context(), group)
	case "heal":
		err = s.chaos.Heal(r.Context())
	case "recover":
		err = s.chaos.Recover(r.Context())
	default:
		writeError(w, http.StatusNotFound, fmt.Sprintf("unknown fault %q", action))
		return
	}

	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "action": action, "node": affected})
}

// --- plumbing ---

// limiter is a per-client token bucket that refills continuously, so a
// visitor who waits gets their budget back gradually rather than in a
// lump at the top of the minute.
type limiter struct {
	perMinute int

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newLimiter(perMinute int) *limiter {
	return &limiter{perMinute: perMinute, buckets: make(map[string]*bucket)}
}

func (l *limiter) allow(key string) bool {
	if l.perMinute <= 0 {
		return true
	}
	now := time.Now()
	capacity := float64(l.perMinute)
	refillPerSecond := capacity / 60

	l.mu.Lock()
	defer l.mu.Unlock()

	// Bounded by discarding buckets that have been idle long enough to
	// have refilled completely: keeping them would let a stream of unique
	// addresses grow the map without limit.
	if len(l.buckets) > 4096 {
		for key, b := range l.buckets {
			if now.Sub(b.last) > time.Minute {
				delete(l.buckets, key)
			}
		}
	}

	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: capacity, last: now}
		l.buckets[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * refillPerSecond
	if b.tokens > capacity {
		b.tokens = capacity
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			if first, _, ok := strings.Cut(fwd, ","); ok {
				return strings.TrimSpace(first)
			}
			return strings.TrimSpace(fwd)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// decodeJSON reads a small JSON body, tolerating an empty one so that
// endpoints taking no arguments can be called with no body at all.
func decodeJSON(r *http.Request, dst any) error {
	body := http.MaxBytesReader(nil, r.Body, 8<<10)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if err.Error() == "EOF" {
			return nil
		}
		return fmt.Errorf("malformed request body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"ok": false, "error": msg})
}

func writeSSE(w http.ResponseWriter, event string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	return err
}

// withCORS opens the API to any origin. The endpoints behind it are the
// same ones the page itself calls and carry no credentials, so nothing is
// protected by the browser refusing a cross-origin read — and allowing it
// means the console can be served from anywhere during development.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// spaHandler serves a built single-page app: real files where they exist,
// index.html everywhere else so client-side routes survive a reload.
func spaHandler(dir string) http.Handler {
	files := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Join(dir, filepath.Clean("/"+r.URL.Path))
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			files.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(dir, "index.html"))
	})
}
