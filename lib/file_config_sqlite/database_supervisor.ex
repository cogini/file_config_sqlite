defmodule FileConfigSqlite.DatabaseSupervisor do
  @moduledoc """
  Supervise database processes.

  This allows them to die without affecting others.

  """
  use Supervisor, restart: :permanent
  require Logger

  def start_link(args, opts \\ []) do
    Supervisor.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    # Logger.debug("args: #{inspect(args)}")

    children = [
      {FileConfigSqlite.Database, args}
    ]

    options = [
      strategy: :one_for_one,
      max_restarts: 1000,
      max_seconds: 300
    ]

    Supervisor.init(children, options)
  end
end
