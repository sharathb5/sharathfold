import Config

# Keep a single partitioner so the target name is Manifold.Partitioner (not _N).
config :manifold,
  partitioners: 1,
  workers_per_partitioner: 2,
  senders: 1
