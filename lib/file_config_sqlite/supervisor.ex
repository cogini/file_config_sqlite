defmodule FileConfigSqlite.Supervisor do
  @moduledoc """
  Top level supervisor for module.

  Start this in your app supervision tree.
  """
  use Supervisor, restart: :permanent

  require Logger

  def start_link(args, opts \\ []) do
    Supervisor.start_link(__MODULE__, args, opts)
  end

  @impl Supervisor
  def init(_args) do
    # Logger.debug("args: #{inspect(args)}")

    children = [
      {Registry, keys: :unique, name: FileConfigSqlite.DatabaseRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: FileConfigSqlite.DatabaseManager},
    ]

    options = [
      strategy: :one_for_one,
      max_restarts: 1000,
      max_seconds: 300,
    ]

    Supervisor.init(children, options)
  end
end
