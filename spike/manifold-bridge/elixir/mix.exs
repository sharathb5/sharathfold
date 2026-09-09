defmodule ManifoldBridge.MixProject do
  use Mix.Project

  def project do
    [
      app: :manifold_bridge,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ManifoldBridge.Application, []}
    ]
  end

  defp deps do
    [
      {:manifold, "~> 1.6"}
    ]
  end
end
