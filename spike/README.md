# spike

Throwaway experiments proving Go↔Erlang distribution pieces. Not imported by
library code.

- `dist-handshake/` — Ergo node speaking Erlang distribution to Elixir (OTP 29)
- `manifold-bridge/` — real `Manifold.send/2` into an Ergo-registered
  `Elixir.Manifold.Partitioner`

The production-shaped path lives under `../manifold/`.
