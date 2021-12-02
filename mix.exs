defmodule FileConfigSqlite.MixProject do
  use Mix.Project

  @github "https://github.com/cogini/file_config_sqlite"

  defp description do
    "SQLite storage module for file_config."
  end

  def project do
    [
      app: :file_config_sqlite,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      build_embedded: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      description: description(),
      package: package(),
      deps: deps(),
      docs: docs(),
      source_url: @github,
      homepage_url: @github,
      dialyzer: [
        plt_add_apps: [:mix],
        # plt_add_deps: :project,
        # plt_add_apps: [:ssl, :mnesia, :compiler, :xmerl, :inets, :disk_log],
        plt_add_deps: true
        # flags: ["-Werror_handling", "-Wrace_conditions"],
        # flags: ["-Wunmatched_returns", :error_handling, :race_conditions, :underspecs],
        # ignore_warnings: "dialyzer.ignore-warnings"
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
      # xref: [
      #   exclude: [EEx, :cover]
      # ],
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :eex]
    ]
  end

  # defp extra_applications(:test), do: []
  # defp extra_applications(_),     do: []

  # Specifies which paths to compile per environment
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      # {:esqlite, github: "mmzeeman/esqlite"},
      {:esqlite, "~> 0.4.1"},
      {:ex_doc, "~> 0.23", only: :dev, runtime: false},
      {:excoveralls, "~> 0.14.0", only: [:dev, :test], runtime: false},
      {:exexec, "~> 0.2.0"},
      # {:exqlite, "~> 0.6.1"},
      # {:exqlite, github: "elixir-sqlite/exqlite"},
      {:exqlite, github: "cogini/exqlite", branch: "typespecs"},
      # {:exqlite, path: "../../build/exqlite"},

      {:nimble_csv, "~> 1.1"},
      # {:file_config, path: "../../file_config"},
      # {:file_config, "~> 0.12.0", only: [:dev, :test], runtime: false},
      {:file_config, github: "cogini/file_config"},
      {:sqlitex, "~> 1.7"}
    ]
  end

  defp package do
    [
      maintainers: ["Jake Morrison"],
      licenses: ["Mozilla Public License 2.0"],
      links: %{"GitHub" => @github}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      source_url: @github,
      # api_reference: false,
      source_url_pattern: "#{@github}/blob/master/%{path}#L%{line}"
    ]
  end
end
