defmodule FoldOrch do
  @moduledoc """
  Elixir orchestrator: real Manifold routes dispatch instructions to Go fold nodes.

  Durable fold_partition_owners (Postgres) is authoritative for which logical
  NodeID may Claim/deliver. This orchestrator looks up that owner, maps it to a
  Go dispatch PID, and uses unmodified Manifold only for cross-node send.
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
  Connect to Go nodes, fan events using durable ownership → Manifold routing,
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

    dispatch_pids = fetch_dispatch_pids!()
    node_id_to_idx = Map.new(@go_nodes, fn {idx, _} -> {"go#{idx}", idx} end)

    subscribers =
      for i <- 0..7 do
        id = "sub-#{i}"
        part = FoldOrch.Assign.partition(id, @partitions)
        owner = owner_of!(hd(@go_nodes) |> elem(1), part)
        idx = Map.fetch!(node_id_to_idx, owner)
        %{id: id, url: "#{hook_base}/#{id}", partition: part, owner: owner, node: idx}
      end

    by_node = Enum.group_by(subscribers, & &1.node)

    Enum.each(by_node, fn {idx, subs} ->
      IO.puts(
        "FOLD ORCH: node #{idx} (#{hd(subs).owner}) owns #{length(subs)} subscribers: #{Enum.map_join(subs, ", ", & &1.id)}"
      )
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

        msg = {:dispatch, payload, self()}
        :ok = Manifold.send([pid], msg)
      end)

      expect_acks!(map_size(by_node))
      IO.puts("FOLD ORCH: event #{n} dispatched to #{map_size(by_node)} nodes via Manifold")
    end

    Process.sleep(2_000)
    IO.puts("FOLD ORCH: PROOF OK — Manifold routed #{events} events; ownership from Postgres")
    :ok
  end

  @doc """
  Graceful handoff e2e: deliver on A, Begin→Complete partition to B, deliver on B,
  preserve per-subscriber sequence order. Prints routing evidence before/after.
  """
  def run_handoff(opts \\ []) do
    hook_base = Keyword.get(opts, :hook_base, "http://127.0.0.1:8099/hook")
    Node.set_cookie(@cookie)

    Enum.each(@go_nodes, fn {_idx, name} ->
      connect!(name)
    end)

    Process.sleep(300)
    dispatch_pids = fetch_dispatch_pids!()
    ctrl_node = elem(hd(@go_nodes), 1)

    # Pick a subscriber whose initial static assignment is go0.
    {sub_id, part} =
      Enum.find_value(0..10_000, fn i ->
        id = "handoff-sub-#{i}"
        p = FoldOrch.Assign.partition(id, @partitions)

        if FoldOrch.Assign.node_index(id, @partitions, @node_count) == 0 do
          {id, p}
        else
          nil
        end
      end) || raise "no subscriber hashing to go0"

    owner_before = owner_of!(ctrl_node, part)
    IO.puts("FOLD ORCH HANDOFF: subscriber=#{sub_id} partition=#{part} owner_before=#{owner_before}")

    unless owner_before == "go0" do
      IO.puts("FOLD ORCH HANDOFF: FAIL expected go0, got #{owner_before}")
      System.halt(1)
    end

    # Event 0 → Manifold → go0 (durable owner).
    dispatch_one!(dispatch_pids, 0, "evt-before", sub_id, "#{hook_base}/#{sub_id}")
    wait_delivery!("go0", 18080, sub_id, 1)
    IO.puts("FOLD ORCH HANDOFF: BEFORE route=go0 delivered seq=1 via Manifold")

    begin_handoff!(ctrl_node, part, "go0", "go1")
    {_, state, next, gen} = owner_row!(ctrl_node, part)
    IO.puts("FOLD ORCH HANDOFF: after Begin state=#{state} next=#{inspect(next)} gen=#{gen}")

    wait_inflight_zero!(ctrl_node, part)
    complete_handoff!(ctrl_node, part)

    owner_after = owner_of!(ctrl_node, part)
    IO.puts("FOLD ORCH HANDOFF: owner_after=#{owner_after}")

    unless owner_after == "go1" do
      IO.puts("FOLD ORCH HANDOFF: FAIL expected go1 after Complete, got #{owner_after}")
      System.halt(1)
    end

    # Event 1 → ownership lookup → go1 → Manifold.
    node_id_to_idx = Map.new(@go_nodes, fn {idx, _} -> {"go#{idx}", idx} end)
    route_idx = Map.fetch!(node_id_to_idx, owner_after)
    IO.puts("FOLD ORCH HANDOFF: routing evt-after to node_idx=#{route_idx} (#{owner_after})")
    dispatch_one!(dispatch_pids, route_idx, "evt-after", sub_id, "#{hook_base}/#{sub_id}")
    wait_delivery!("go1", 18081, sub_id, 1)
    IO.puts("FOLD ORCH HANDOFF: AFTER route=#{owner_after} delivered seq=2 via Manifold")

    # Cross-node sequence: go0 has seq 1, go1 has seq 2 for same subscriber.
    verify_handoff_order!(sub_id)

    IO.puts(
      "FOLD ORCH HANDOFF: PROOF OK — #{sub_id} part=#{part} go0→go1; Manifold routed before/after; order preserved"
    )

    :ok
  end

  defp fetch_dispatch_pids! do
    Map.new(@go_nodes, fn {idx, name} ->
      pid = fetch_dispatch_pid!(name)
      IO.puts("FOLD ORCH: node=#{idx} #{name} dispatch=#{inspect(pid)}")
      {idx, pid}
    end)
  end

  defp dispatch_one!(dispatch_pids, node_idx, event_id, sub_id, url) do
    pid = Map.fetch!(dispatch_pids, node_idx)

    payload =
      Jason.encode!(%{
        event: %{id: event_id, type: "handoff.event", data: %{id: event_id}},
        subscribers: [%{id: sub_id, url: url}]
      })

    :ok = Manifold.send([pid], {:dispatch, payload, self()})
    expect_acks!(1)
  end

  defp owner_of!(node, partition) do
    {owner, _state, _next, _gen} = owner_row!(node, partition)
    owner
  end

  defp owner_row!(node, partition) do
    send({:fold_dispatch, node}, {:owner_of, partition, self()})

    receive do
      {:owner, ^partition, owner, state, next, gen} ->
        {bin(owner), bin(state), bin(next), gen}

      {:owner_err, reason} ->
        IO.puts("FOLD ORCH: FAIL owner_err #{inspect(reason)}")
        System.halt(1)
    after
      @timeout ->
        IO.puts("FOLD ORCH: FAIL timed out owner_of partition=#{partition}")
        System.halt(1)
    end
  end

  # Go ETF strings may arrive as binaries or charlists depending on encoding.
  defp bin(v) when is_binary(v), do: v
  defp bin(v) when is_list(v), do: List.to_string(v)
  defp bin(_), do: ""

  defp begin_handoff!(node, partition, from_node, to_node) do
    send({:fold_dispatch, node}, {:begin_handoff, partition, from_node, to_node, self()})
    await_handoff_ok!("BeginHandoff")
  end

  defp complete_handoff!(node, partition) do
    send({:fold_dispatch, node}, {:complete_handoff, partition, self()})
    await_handoff_ok!("CompleteHandoff")
  end

  defp await_handoff_ok!(label) do
    receive do
      :handoff_ok ->
        :ok

      {:handoff_err, reason} ->
        IO.puts("FOLD ORCH: FAIL #{label} #{inspect(reason)}")
        System.halt(1)
    after
      @timeout ->
        IO.puts("FOLD ORCH: FAIL timed out #{label}")
        System.halt(1)
    end
  end

  defp wait_inflight_zero!(node, partition) do
    deadline = System.monotonic_time(:millisecond) + 10_000

    Stream.repeatedly(fn ->
      send({:fold_dispatch, node}, {:inflight_count, partition, self()})

      receive do
        {:inflight, n} when is_integer(n) -> n
        {:inflight_err, reason} ->
          IO.puts("FOLD ORCH: FAIL inflight_err #{inspect(reason)}")
          System.halt(1)
      after
        @timeout ->
          IO.puts("FOLD ORCH: FAIL timed out inflight_count")
          System.halt(1)
      end
    end)
    |> Enum.reduce_while(nil, fn n, _ ->
      if n == 0 do
        {:halt, :ok}
      else
        if System.monotonic_time(:millisecond) > deadline do
          IO.puts("FOLD ORCH: FAIL timed out waiting in_flight=0 (last=#{n})")
          System.halt(1)
        end

        Process.sleep(20)
        {:cont, n}
      end
    end)
  end

  defp wait_delivery!(label, port, sub_id, min_count) do
    deadline = System.monotonic_time(:millisecond) + 10_000

    Stream.repeatedly(fn ->
      Process.sleep(50)
      deliveries_for(port, sub_id)
    end)
    |> Enum.reduce_while([], fn dels, _ ->
      if length(dels) >= min_count do
        {:halt, dels}
      else
        if System.monotonic_time(:millisecond) > deadline do
          IO.puts("FOLD ORCH: FAIL timed out waiting #{label} delivery for #{sub_id}")
          System.halt(1)
        end

        {:cont, dels}
      end
    end)
  end

  defp deliveries_for(port, sub_id) do
    url = "http://127.0.0.1:#{port}/deliveries"
    {body, 0} = System.cmd("curl", ["-fsS", url])
    body |> Jason.decode!() |> Enum.filter(&(&1["subscriber_id"] == sub_id))
  end

  defp verify_handoff_order!(sub_id) do
    a = deliveries_for(18080, sub_id)
    b = deliveries_for(18081, sub_id)
    seqs = Enum.map(a ++ b, & &1["sequence"]) |> Enum.sort()

    unless seqs == [1, 2] do
      IO.puts("FOLD ORCH HANDOFF: FAIL sequences=#{inspect(seqs)} want [1,2] a=#{inspect(a)} b=#{inspect(b)}")
      System.halt(1)
    end

    unless length(a) == 1 and hd(a)["sequence"] == 1 do
      IO.puts("FOLD ORCH HANDOFF: FAIL go0 should have only seq 1, got #{inspect(a)}")
      System.halt(1)
    end

    unless length(b) == 1 and hd(b)["sequence"] == 2 do
      IO.puts("FOLD ORCH HANDOFF: FAIL go1 should have only seq 2, got #{inspect(b)}")
      System.halt(1)
    end
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
