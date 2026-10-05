// Package main is the Go worker node for the Manifold scale-out path.
//
// It joins the Erlang cluster via Ergo, impersonates Manifold.Partitioner, and
// runs fold against shared Postgres. Durable fold_partition_owners is
// authoritative for Claim; Manifold only routes dispatch messages to nodes.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/gen"
	"ergo.services/proto/erlang23"
	"ergo.services/proto/erlang23/dist"
	"ergo.services/proto/erlang23/epmd"
	"ergo.services/proto/erlang23/etf"
	"ergo.services/proto/erlang23/handshake"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/postgres"
)

const (
	defaultCookie     = "fold"
	partitionerName   = gen.Atom("Elixir.Manifold.Partitioner")
	dispatchName      = gen.Atom("fold_dispatch")
	defaultPartitions = 256
)

func main() {
	var (
		nodeName   = flag.String("name", "go0@localhost", "Erlang node name")
		cookie     = flag.String("cookie", defaultCookie, "distribution cookie")
		dsn        = flag.String("dsn", envOr("FOLD_PG_DSN", "postgres://localhost/fold_test?sslmode=disable"), "Postgres DSN")
		nodeIndex  = flag.Int("node", 0, "this node's index in the fleet [0, nodes)")
		nodeCount  = flag.Int("nodes", 2, "total Go nodes in the fleet")
		partitions = flag.Int("partitions", defaultPartitions, "fold partition hash space")
		workers    = flag.Int("workers", 2, "local fold workers")
		httpAddr   = flag.String("http", "", "optional status HTTP listen addr")
		recordOnly = flag.Bool("record", false, "succeed deliveries without HTTP (e2e)")
	)
	flag.Parse()

	if *nodeCount < 1 || *nodeIndex < 0 || *nodeIndex >= *nodeCount {
		log.Fatalf("invalid node=%d nodes=%d", *nodeIndex, *nodeCount)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pg, err := postgres.Open(ctx, *dsn)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pg.Close()

	nodeID := fold.LogicalNodeID(*nodeIndex)
	owners := fold.OwnerAssignmentsForNodes(*partitions, *nodeCount)
	if err := store.PartitionOwnership(pg).EnsureOwnerAssignments(ctx, owners); err != nil {
		log.Fatalf("EnsureOwnerAssignments: %v", err)
	}

	claim := fold.PartitionsForNode(*partitions, *nodeCount, *nodeIndex)
	var inner fold.Transport
	if *recordOnly {
		inner = &fold.RecordingTransport{}
	} else {
		inner = fold.NewHTTPTransport(0, nil)
	}
	rec := newRecordingTransport(inner)
	d, err := fold.New(fold.Config{
		Store:           pg,
		Transport:       rec,
		Workers:         *workers,
		Partitions:      *partitions,
		PartitionsClaim: claim,
		NodeID:          nodeID,
		IDPrefix:        fmt.Sprintf("n%d-", *nodeIndex),
	})
	if err != nil {
		log.Fatalf("fold.New: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		log.Fatalf("fold.Start: %v", err)
	}
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_ = d.Close(cctx)
	}()

	b := &bridge{
		dispatcher: d,
		pg:         pg,
		nodeID:     nodeID,
		nodeIndex:  *nodeIndex,
		nodeCount:  *nodeCount,
		partitions: *partitions,
		record:     rec,
	}

	var options gen.NodeOptions
	options.Log.DefaultLogger.Disable = true
	options.Network.Cookie = *cookie
	options.Network.Registrar = epmd.Create(epmd.Options{})
	options.Network.Handshake = handshake.Create(handshake.Options{})
	options.Network.Proto = dist.Create(dist.Options{})

	node, err := ergo.StartNode(gen.Atom(*nodeName), options)
	if err != nil {
		log.Fatalf("StartNode: %v", err)
	}

	if _, err := node.SpawnRegister(partitionerName, b.factoryPartitioner, gen.ProcessOptions{}); err != nil {
		log.Fatalf("register partitioner: %v", err)
	}
	dispPID, err := node.SpawnRegister(dispatchName, b.factoryDispatch, gen.ProcessOptions{})
	if err != nil {
		log.Fatalf("register fold_dispatch: %v", err)
	}

	log.Printf("fold node up name=%s nodeID=%s node=%d/%d claim=%d dispatch=%s",
		*nodeName, nodeID, *nodeIndex, *nodeCount, len(claim), dispPID)

	if *httpAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		mux.HandleFunc("/deliveries", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(rec.snapshot())
		})
		mux.HandleFunc("/ownership", b.handleOwnershipHTTP)
		go func() {
			log.Printf("http status on %s", *httpAddr)
			if err := http.ListenAndServe(*httpAddr, mux); err != nil {
				log.Printf("http: %v", err)
			}
		}()
	}

	<-ctx.Done()
	node.Stop()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type bridge struct {
	dispatcher *fold.Dispatcher
	pg         *postgres.Store
	nodeID     string
	nodeIndex  int
	nodeCount  int
	partitions int
	record     *recordingTransport
}

func (b *bridge) factoryPartitioner() gen.ProcessBehavior {
	return &partitionerActor{bridge: b}
}

func (b *bridge) factoryDispatch() gen.ProcessBehavior {
	return &dispatchActor{bridge: b}
}

func (b *bridge) handleOwnershipHTTP(w http.ResponseWriter, r *http.Request) {
	var part int
	if _, err := fmt.Sscanf(r.URL.Query().Get("partition"), "%d", &part); err != nil {
		http.Error(w, "partition query required", http.StatusBadRequest)
		return
	}
	owner, state, next, gen, err := b.pg.OwnerRow(r.Context(), part)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"partition":  part,
		"owner_node": owner,
		"state":      state,
		"next_owner": next,
		"generation": gen,
	})
}

// partitionerActor impersonates Manifold.Partitioner on this Go node.
type partitionerActor struct {
	erlang23.GenServer
	bridge *bridge
}

func (p *partitionerActor) HandleCast(message any) error {
	tuple, ok := message.(etf.Tuple)
	if !ok || len(tuple) != 3 {
		return nil
	}
	tag, ok := tuple.Element(1).(gen.Atom)
	if !ok || tag != "send" {
		return nil
	}
	msg := tuple.Element(3)
	for _, pid := range pidList(tuple.Element(2)) {
		if err := p.Send(pid, msg); err != nil {
			log.Printf("partitioner forward to %s: %v", pid, err)
		}
	}
	return nil
}

func (p *partitionerActor) HandleInfo(message any) error { return nil }

// dispatchActor receives {:dispatch, event, subscribers} and ownership/handoff RPCs.
type dispatchActor struct {
	erlang23.GenServer
	bridge *bridge
}

func (d *dispatchActor) HandleInfo(message any) error {
	tuple, ok := message.(etf.Tuple)
	if !ok || len(tuple) < 1 {
		return nil
	}
	tag, ok := tuple.Element(1).(gen.Atom)
	if !ok {
		return nil
	}

	switch tag {
	case "give_pid":
		if len(tuple) < 2 {
			return nil
		}
		from, ok := tuple.Element(2).(gen.PID)
		if !ok {
			return nil
		}
		_ = d.Send(from, etf.Tuple{gen.Atom("pid"), d.PID()})
		return nil

	case "dispatch":
		return d.handleDispatch(tuple)

	case "owner_of":
		return d.handleOwnerOf(tuple)

	case "begin_handoff":
		return d.handleBeginHandoff(tuple)

	case "complete_handoff":
		return d.handleCompleteHandoff(tuple)

	case "inflight_count":
		return d.handleInFlightCount(tuple)
	}
	return nil
}

func (d *dispatchActor) handleOwnerOf(tuple etf.Tuple) error {
	// {:owner_of, partition, from_pid}
	if len(tuple) < 3 {
		return nil
	}
	part, ok := asInt(tuple.Element(2))
	if !ok {
		return nil
	}
	from, ok := tuple.Element(3).(gen.PID)
	if !ok {
		return nil
	}
	owner, state, next, generation, err := d.bridge.pg.OwnerRow(context.Background(), part)
	if err != nil {
		_ = d.Send(from, etf.Tuple{gen.Atom("owner_err"), etfBinary(err.Error())})
		return nil
	}
	// []byte encodes as ETF binary so Elixir sees binaries, not charlists.
	_ = d.Send(from, etf.Tuple{
		gen.Atom("owner"),
		part,
		etfBinary(owner),
		etfBinary(state),
		etfBinary(next),
		int64(generation),
	})
	return nil
}

func (d *dispatchActor) handleBeginHandoff(tuple etf.Tuple) error {
	// {:begin_handoff, partition, from_node, to_node, from_pid}
	if len(tuple) < 5 {
		return nil
	}
	part, ok := asInt(tuple.Element(2))
	if !ok {
		return nil
	}
	fromNode, ok := asString(tuple.Element(3))
	if !ok {
		return nil
	}
	toNode, ok := asString(tuple.Element(4))
	if !ok {
		return nil
	}
	from, ok := tuple.Element(5).(gen.PID)
	if !ok {
		return nil
	}
	err := store.PartitionOwnership(d.bridge.pg).BeginHandoff(context.Background(), part, fromNode, toNode)
	if err != nil {
		_ = d.Send(from, etf.Tuple{gen.Atom("handoff_err"), etfBinary(err.Error())})
		return nil
	}
	_ = d.Send(from, gen.Atom("handoff_ok"))
	return nil
}

func (d *dispatchActor) handleCompleteHandoff(tuple etf.Tuple) error {
	// {:complete_handoff, partition, from_pid}
	if len(tuple) < 3 {
		return nil
	}
	part, ok := asInt(tuple.Element(2))
	if !ok {
		return nil
	}
	from, ok := tuple.Element(3).(gen.PID)
	if !ok {
		return nil
	}
	err := store.PartitionOwnership(d.bridge.pg).CompleteHandoff(context.Background(), part)
	if err != nil {
		_ = d.Send(from, etf.Tuple{gen.Atom("handoff_err"), etfBinary(err.Error())})
		return nil
	}
	_ = d.Send(from, gen.Atom("handoff_ok"))
	return nil
}

func (d *dispatchActor) handleInFlightCount(tuple etf.Tuple) error {
	// {:inflight_count, partition, from_pid}
	if len(tuple) < 3 {
		return nil
	}
	part, ok := asInt(tuple.Element(2))
	if !ok {
		return nil
	}
	from, ok := tuple.Element(3).(gen.PID)
	if !ok {
		return nil
	}
	n, err := d.bridge.pg.InFlightCount(context.Background(), part)
	if err != nil {
		_ = d.Send(from, etf.Tuple{gen.Atom("inflight_err"), etfBinary(err.Error())})
		return nil
	}
	_ = d.Send(from, etf.Tuple{gen.Atom("inflight"), int64(n)})
	return nil
}

func (d *dispatchActor) handleDispatch(tuple etf.Tuple) error {
	// {:dispatch, json_binary, from_pid}
	if len(tuple) < 3 {
		log.Printf("dispatch: short tuple len=%d", len(tuple))
		return nil
	}
	raw, err := asBytes(tuple.Element(2))
	if err != nil {
		log.Printf("dispatch payload: %v", err)
		replyDispatch(d, tuple, false, err.Error())
		return nil
	}
	var wire struct {
		Event struct {
			ID   string          `json:"id"`
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		} `json:"event"`
		Subscribers []struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		} `json:"subscribers"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		log.Printf("dispatch json: %v", err)
		replyDispatch(d, tuple, false, err.Error())
		return nil
	}
	ev := fold.Event{ID: wire.Event.ID, Type: wire.Event.Type, Payload: wire.Event.Data}
	subs := make([]fold.Subscriber, 0, len(wire.Subscribers))
	for _, s := range wire.Subscribers {
		subs = append(subs, fold.Subscriber{ID: s.ID, URL: s.URL})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Accept only subscribers whose durable partition owner is this NodeID.
	filtered := make([]fold.Subscriber, 0, len(subs))
	for _, s := range subs {
		part := fold.NodeIndex(s.ID, d.bridge.partitions, d.bridge.partitions)
		owner, _, _, _, err := d.bridge.pg.OwnerRow(ctx, part)
		if err != nil {
			log.Printf("dispatch owner lookup sub=%s part=%d: %v", s.ID, part, err)
			replyDispatch(d, tuple, false, err.Error())
			return nil
		}
		if owner == d.bridge.nodeID {
			filtered = append(filtered, s)
		}
	}
	if len(filtered) == 0 {
		replyDispatch(d, tuple, true, "")
		return nil
	}

	if err := d.bridge.dispatcher.Dispatch(ctx, ev, filtered); err != nil {
		log.Printf("fold.Dispatch: %v", err)
		replyDispatch(d, tuple, false, err.Error())
		return nil
	}
	replyDispatch(d, tuple, true, "")
	return nil
}

func replyDispatch(d *dispatchActor, tuple etf.Tuple, ok bool, errMsg string) {
	from, isPID := tuple.Element(3).(gen.PID)
	if !isPID {
		log.Printf("dispatch: no reply pid")
		return
	}
	if ok {
		if err := d.Send(from, gen.Atom("dispatch_ok")); err != nil {
			log.Printf("dispatch_ok send: %v", err)
		}
		return
	}
	_ = d.Send(from, etf.Tuple{gen.Atom("dispatch_err"), etfBinary(errMsg)})
}

func asBytes(v any) ([]byte, error) {
	switch b := v.(type) {
	case string:
		return []byte(b), nil
	case []byte:
		return b, nil
	default:
		return nil, fmt.Errorf("want binary, got %T", v)
	}
}

// asString accepts Go string or ETF binary ([]byte) from Elixir binaries.
func asString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

// etfBinary encodes a Go string as an ETF binary (Elixir binary, not charlist).
func etfBinary(s string) []byte {
	return []byte(s)
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case uint64:
		return int(n), true
	default:
		return 0, false
	}
}

func pidList(v any) []gen.PID {
	switch xs := v.(type) {
	case etf.List:
		out := make([]gen.PID, 0, len(xs))
		for _, x := range xs {
			if pid, ok := x.(gen.PID); ok {
				out = append(out, pid)
			}
		}
		return out
	case []any:
		out := make([]gen.PID, 0, len(xs))
		for _, x := range xs {
			if pid, ok := x.(gen.PID); ok {
				out = append(out, pid)
			}
		}
		return out
	case gen.PID:
		return []gen.PID{xs}
	default:
		return nil
	}
}

type recordingTransport struct {
	inner fold.Transport
	mu    sync.Mutex
	calls []recorded
}

type recorded struct {
	SubscriberID string `json:"subscriber_id"`
	EventID      string `json:"event_id"`
	Sequence     int64  `json:"sequence"`
	URL          string `json:"url"`
}

func newRecordingTransport(inner fold.Transport) *recordingTransport {
	return &recordingTransport{inner: inner}
}

func (r *recordingTransport) Deliver(ctx context.Context, d store.Delivery) fold.Result {
	res := r.inner.Deliver(ctx, d)
	if res.Outcome == fold.OutcomeSuccess {
		r.mu.Lock()
		r.calls = append(r.calls, recorded{
			SubscriberID: d.SubscriberID,
			EventID:      d.EventID,
			Sequence:     d.Sequence,
			URL:          d.URL,
		})
		r.mu.Unlock()
	}
	return res
}

func (r *recordingTransport) snapshot() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recorded, len(r.calls))
	copy(out, r.calls)
	return out
}
