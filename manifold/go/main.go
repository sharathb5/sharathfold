// Package main is the Go worker node for the Manifold scale-out path (D12).
//
// It joins the Erlang cluster via Ergo, impersonates Manifold.Partitioner, and
// runs fold against shared Postgres claiming only this node's partition slice.
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

	log.Printf("fold node up name=%s node=%d/%d claim=%d dispatch=%s",
		*nodeName, *nodeIndex, *nodeCount, len(claim), dispPID)

	if *httpAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		mux.HandleFunc("/deliveries", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(rec.snapshot())
		})
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

// dispatchActor receives {:dispatch, event, subscribers} (and pid exchange).
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
	}
	return nil
}

func (d *dispatchActor) handleDispatch(tuple etf.Tuple) error {
	// {:dispatch, json_binary, from_pid}
	// json: {"event":{...},"subscribers":[{"id":"...","url":"..."}]}
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

	filtered := make([]fold.Subscriber, 0, len(subs))
	for _, s := range subs {
		if fold.NodeIndex(s.ID, d.bridge.partitions, d.bridge.nodeCount) == d.bridge.nodeIndex {
			filtered = append(filtered, s)
		}
	}
	if len(filtered) == 0 {
		replyDispatch(d, tuple, true, "")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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
	_ = d.Send(from, etf.Tuple{gen.Atom("dispatch_err"), errMsg})
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
