defmodule FileConfigSqlite.MixProject do
  use Mix.Project

  def project do
    [
      app: :file_config_sqlite,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :lager]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:esqlite, github: "mmzeeman/esqlite"},
      {:esqlite, "~> 0.4.0"},
      {:nimble_csv, "~> 0.3"},
      {:sqlitex, "~> 1.7"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
