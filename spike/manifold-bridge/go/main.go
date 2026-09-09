// Spike: can Ergo impersonate Manifold.Partitioner so real Manifold.send/2
// from Elixir delivers to PIDs on this Go node?
package main

import (
	"fmt"
	"os"
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/gen"
	"ergo.services/proto/erlang23"
	"ergo.services/proto/erlang23/dist"
	"ergo.services/proto/erlang23/epmd"
	"ergo.services/proto/erlang23/etf"
	"ergo.services/proto/erlang23/handshake"
)

const (
	nodeName        = "go@localhost"
	cookie          = "spike"
	partitionerName = gen.Atom("Elixir.Manifold.Partitioner")
	receiverName    = gen.Atom("receiver")
)

func main() {
	var options gen.NodeOptions
	options.Log.DefaultLogger.TimeFormat = time.TimeOnly
	options.Network.Cookie = cookie
	options.Network.Registrar = epmd.Create(epmd.Options{})
	options.Network.Handshake = handshake.Create(handshake.Options{})
	options.Network.Proto = dist.Create(dist.Options{})

	node, err := ergo.StartNode(gen.Atom(nodeName), options)
	if err != nil {
		fmt.Fprintf(os.Stderr, "StartNode failed: %v\n", err)
		os.Exit(1)
	}

	partPID, err := node.SpawnRegister(partitionerName, factoryPartitioner, gen.ProcessOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "SpawnRegister partitioner failed: %v\n", err)
		os.Exit(1)
	}

	recvPID, err := node.SpawnRegister(receiverName, factoryReceiver, gen.ProcessOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "SpawnRegister receiver failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("SPIKE: Go node up name=%s cookie=%s\n", node.Name(), cookie)
	fmt.Printf("SPIKE: partitioner registered=%s pid=%s\n", partitionerName, partPID)
	fmt.Printf("SPIKE: receiver registered=%s pid=%s\n", receiverName, recvPID)
	fmt.Printf("SPIKE: waiting for Manifold cast from Elixir...\n")

	node.Wait()
}

func factoryPartitioner() gen.ProcessBehavior { return &partitioner{} }
func factoryReceiver() gen.ProcessBehavior    { return &receiver{} }

// partitioner impersonates Manifold.Partitioner: accepts GenServer.cast
// {:send, pids, message} and forwards message to each listed PID.
type partitioner struct {
	erlang23.GenServer
}

func (p *partitioner) HandleCast(message any) error {
	fmt.Printf("SPIKE PARTITIONER CAST: type=%T value=%#v\n", message, message)

	tuple, ok := message.(etf.Tuple)
	if !ok || len(tuple) != 3 {
		fmt.Printf("SPIKE PARTITIONER: unexpected cast shape\n")
		return nil
	}
	tag, ok := tuple.Element(1).(gen.Atom)
	if !ok || tag != "send" {
		fmt.Printf("SPIKE PARTITIONER: not {:send, ...}\n")
		return nil
	}

	msg := tuple.Element(3)
	for _, pid := range pidList(tuple.Element(2)) {
		if err := p.Send(pid, msg); err != nil {
			fmt.Printf("SPIKE PARTITIONER SEND ERR pid=%s: %v\n", pid, err)
			continue
		}
		fmt.Printf("SPIKE PARTITIONER FORWARDED to %s\n", pid)
	}
	return nil
}

func (p *partitioner) HandleInfo(message any) error {
	fmt.Printf("SPIKE PARTITIONER INFO: type=%T value=%#v\n", message, message)
	return nil
}

type receiver struct {
	erlang23.GenServer
}

func (r *receiver) HandleInfo(message any) error {
	fmt.Printf("SPIKE RECEIVER: type=%T value=%#v\n", message, message)

	// PID exchange: Elixir asks {:give_pid, from}; we reply {:pid, self()}.
	if tuple, ok := message.(etf.Tuple); ok && len(tuple) == 2 {
		if tag, ok := tuple.Element(1).(gen.Atom); ok && tag == "give_pid" {
			from, ok := tuple.Element(2).(gen.PID)
			if !ok {
				return nil
			}
			reply := etf.Tuple{gen.Atom("pid"), r.PID()}
			if err := r.Send(from, reply); err != nil {
				fmt.Printf("SPIKE RECEIVER give_pid ERR: %v\n", err)
			} else {
				fmt.Printf("SPIKE RECEIVER: gave pid %s to %s\n", r.PID(), from)
			}
			return nil
		}
	}

	// Proof payload arriving via Manifold.Partitioner forward.
	if tuple, ok := message.(etf.Tuple); ok && len(tuple) >= 1 {
		if tag, ok := tuple.Element(1).(gen.Atom); ok && tag == "manifold_proof" {
			fmt.Printf("SPIKE PROOF OK: received manifold_proof via Manifold path\n")
			if len(tuple) >= 2 {
				if from, ok := tuple.Element(2).(gen.PID); ok {
					_ = r.Send(from, gen.Atom("proof_ok"))
				}
			}
		}
	}
	return nil
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
		fmt.Printf("SPIKE PARTITIONER: unknown pid list type %T\n", v)
		return nil
	}
}
