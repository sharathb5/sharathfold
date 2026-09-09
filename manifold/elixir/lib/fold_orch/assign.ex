defmodule FoldOrch.Assign do
  @moduledoc """
  Subscriber→node assignment matching fold.NodeIndex / internal/hash (FNV-1a 64).
  """

  @fnv_offset 0xCBF29CE484222325
  @fnv_prime 0x100000001B3
  @mask 0xFFFFFFFFFFFFFFFF

  @doc "Partition in `[0, partitions)` for subscriber_id (Go `hash.Partition`)."
  def partition(subscriber_id, partitions \\ 256) when partitions > 0 do
    rem(fnv1a64(subscriber_id), partitions)
  end

  @doc "Node index in `[0, node_count)` — Go `fold.NodeIndex`."
  def node_index(subscriber_id, partitions \\ 256, node_count \\ 2)
      when node_count > 0 do
    rem(partition(subscriber_id, partitions), node_count)
  end

  def fnv1a64(str) when is_binary(str) do
    Enum.reduce(:binary.bin_to_list(str), @fnv_offset, fn byte, h ->
      Bitwise.band(Bitwise.bxor(h, byte) * @fnv_prime, @mask)
    end)
  end
end
