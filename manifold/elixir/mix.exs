defmodule FoldOrch.MixProject do
  use Mix.Project

  def project do
    [
      app: :fold_orch,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {FoldOrch.Application, []}
    ]
  end

  defp deps do
    [
      {:manifold, "~> 1.6"},
      {:jason, "~> 1.4"}
    ]
  end
end
