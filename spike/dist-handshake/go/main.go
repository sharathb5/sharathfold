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
	nodeName = "go@localhost"
	cookie   = "spike"
	echoName = gen.Atom("echo")
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

	pid, err := node.SpawnRegister(echoName, factoryEcho, gen.ProcessOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "SpawnRegister echo failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("SPIKE: Go node up name=%s pid=%s registered=%s cookie=%s\n", node.Name(), pid, echoName, cookie)
	fmt.Printf("SPIKE: from Elixir: iex --sname elixir --cookie %s\n", cookie)
	fmt.Printf("SPIKE: then: send({:echo, :\"%s\"}, {:hello_from_elixir, self()})\n", node.Name())

	node.Wait()
}

func factoryEcho() gen.ProcessBehavior {
	return &echo{}
}

type echo struct {
	erlang23.GenServer
}

func (e *echo) HandleInfo(message any) error {
	fmt.Printf("SPIKE RECV: type=%T value=%#v\n", message, message)

	tuple, ok := message.(etf.Tuple)
	if !ok || len(tuple) != 2 {
		return nil
	}
	atom, ok := tuple.Element(1).(gen.Atom)
	if !ok || atom != "hello_from_elixir" {
		return nil
	}
	from, ok := tuple.Element(2).(gen.PID)
	if !ok {
		return nil
	}
	if err := e.Send(from, gen.Atom("hello_from_go")); err != nil {
		fmt.Printf("SPIKE REPLY ERR: %v\n", err)
		return nil
	}
	fmt.Printf("SPIKE REPLY: sent hello_from_go to %s\n", from)
	return nil
}
