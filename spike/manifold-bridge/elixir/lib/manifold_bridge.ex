defmodule ManifoldBridge do
  @moduledoc """
  Spike driver: connect to Ergo Go node, obtain a Go PID, call real Manifold.send/2.
  """

  @go_node :"go@localhost"
  @cookie :spike
  @timeout 5_000

  def run do
    Node.set_cookie(@cookie)

    IO.puts("SPIKE ELIXIR: connecting to #{@go_node}...")

    case Node.connect(@go_node) do
      true ->
        IO.puts("SPIKE ELIXIR: connected; nodes=#{inspect(Node.list())}")

      false ->
        IO.puts("SPIKE ELIXIR: Node.connect failed")
        System.halt(1)

      :ignored ->
        IO.puts("SPIKE ELIXIR: Node.connect ignored (distribution not started?)")
        System.halt(1)
    end

    # Wait briefly for Ergo registration to be usable.
    Process.sleep(200)

    go_pid = fetch_go_pid!()
    IO.puts("SPIKE ELIXIR: got Go pid=#{inspect(go_pid)} node=#{node(go_pid)}")

    unless node(go_pid) == @go_node do
      IO.puts("SPIKE ELIXIR: FAIL pid not on Go node")
      System.halt(1)
    end

    msg = {:manifold_proof, self()}
    IO.puts("SPIKE ELIXIR: Manifold.send([pid], #{inspect(msg)})")
    :ok = Manifold.send([go_pid], msg)

    receive do
      :proof_ok ->
        IO.puts("SPIKE ELIXIR: PROOF OK — Manifold path delivered and Go replied")
        :ok
    after
      @timeout ->
        IO.puts("SPIKE ELIXIR: FAIL timed out waiting for proof_ok")
        System.halt(1)
    end
  end

  defp fetch_go_pid! do
    send({:receiver, @go_node}, {:give_pid, self()})

    receive do
      {:pid, pid} when is_pid(pid) -> pid
    after
      @timeout ->
        IO.puts("SPIKE ELIXIR: FAIL timed out waiting for Go pid")
        System.halt(1)
    end
  end
end
