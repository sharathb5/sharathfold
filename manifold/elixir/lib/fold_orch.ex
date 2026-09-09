defmodule FoldOrch do
  @moduledoc """
  Elixir orchestrator: real Manifold routes dispatch instructions to Go fold nodes.

  Manifold answers where (D12). Each Go node runs fold against shared Postgres and
  enforces head-of-line locally.
  """

  @cookie :fold
  @partitions 256
  @node_count 2
  @timeout 10_000

  @go_nodes [
    {0, :"go0@localhost"},
    {1, :"go1@localhost"}
  ]

  @doc """
  Connect to Go nodes, fan one event across subscribers on both nodes via Manifold,
  wait for dispatch acks, then print PROOF OK.
  """
  def run(opts \\ []) do
    hook_base = Keyword.get(opts, :hook_base, "http://127.0.0.1:8099/hook")
    events = Keyword.get(opts, :events, 5)
    Node.set_cookie(@cookie)

    Enum.each(@go_nodes, fn {_idx, name} ->
      connect!(name)
    end)

    Process.sleep(300)

    dispatch_pids =
      Map.new(@go_nodes, fn {idx, name} ->
        pid = fetch_dispatch_pid!(name)
        IO.puts("FOLD ORCH: node=#{idx} #{name} dispatch=#{inspect(pid)}")
        {idx, pid}
      end)

    # Subscribers spread across both nodes (verified via Assign).
    subscribers =
      for i <- 0..7 do
        id = "sub-#{i}"
        %{id: id, url: "#{hook_base}/#{id}", node: FoldOrch.Assign.node_index(id, @partitions, @node_count)}
      end

    by_node = Enum.group_by(subscribers, & &1.node)

    Enum.each(by_node, fn {idx, subs} ->
      IO.puts("FOLD ORCH: node #{idx} owns #{length(subs)} subscribers: #{Enum.map_join(subs, ", ", & &1.id)}")
    end)

    unless map_size(by_node) == @node_count do
      IO.puts("FOLD ORCH: FAIL need subscribers on both nodes, got #{inspect(Map.keys(by_node))}")
      System.halt(1)
    end

    for n <- 0..(events - 1) do
      event = %{
        id: "evt-#{n}",
        type: "demo.event",
        data: %{n: n}
      }

      Enum.each(by_node, fn {idx, subs} ->
        pid = Map.fetch!(dispatch_pids, idx)

        payload =
          Jason.encode!(%{
            event: event,
            subscribers: Enum.map(subs, fn s -> %{id: s.id, url: s.url} end)
          })

        # JSON binary avoids nested ETF map quirks on the Ergo decode path.
        msg = {:dispatch, payload, self()}
        :ok = Manifold.send([pid], msg)
      end)

      expect_acks!(map_size(by_node))
      IO.puts("FOLD ORCH: event #{n} dispatched to #{map_size(by_node)} nodes")
    end

    # Give workers time to deliver.
    Process.sleep(2_000)
    IO.puts("FOLD ORCH: PROOF OK — Manifold routed #{events} events across #{@node_count} Go nodes")
    :ok
  end

  defp connect!(name) do
    IO.puts("FOLD ORCH: connecting to #{name}...")

    case Node.connect(name) do
      true ->
        IO.puts("FOLD ORCH: connected; nodes=#{inspect(Node.list())}")

      false ->
        IO.puts("FOLD ORCH: Node.connect(#{name}) failed")
        System.halt(1)

      :ignored ->
        IO.puts("FOLD ORCH: Node.connect ignored — start with --sname/--cookie")
        System.halt(1)
    end
  end

  defp fetch_dispatch_pid!(node) do
    send({:fold_dispatch, node}, {:give_pid, self()})

    receive do
      {:pid, pid} when is_pid(pid) -> pid
    after
      @timeout ->
        IO.puts("FOLD ORCH: FAIL timed out waiting for fold_dispatch on #{node}")
        System.halt(1)
    end
  end

  defp expect_acks!(count) do
    expect_acks!(count, 0)
  end

  defp expect_acks!(0, _), do: :ok

  defp expect_acks!(left, got) do
    receive do
      :dispatch_ok ->
        expect_acks!(left - 1, got + 1)

      {:dispatch_err, reason} ->
        IO.puts("FOLD ORCH: FAIL dispatch_err #{inspect(reason)}")
        System.halt(1)
    after
      @timeout ->
        IO.puts("FOLD ORCH: FAIL timed out waiting for dispatch_ok (got #{got})")
        System.halt(1)
    end
  end
end
