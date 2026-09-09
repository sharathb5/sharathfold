import Config

# Single partitioner so Go impersonates Elixir.Manifold.Partitioner (not _N).
config :manifold,
  partitioners: 1,
  workers_per_partitioner: 2,
  senders: 1
