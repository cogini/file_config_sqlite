defmodule FileConfigSqlite.Database do
  @moduledoc ~S"""
  Top level API for database operations and shard database handler.

  """
  use GenServer

  require Logger

  alias FileConfigSqlite.DatabaseRegistry

  @call_timeout 30_000

  # API

  def start_link(args, opts \\ []) do
    # Logger.debug("args: #{inspect(args)}")
    id = {args[:name], args[:shard]}
    name = {:via, Registry, {DatabaseRegistry, id}}
    # Logger.debug("name: #{inspect(name)}")
    opts = Keyword.put_new(opts, :name, name)
    GenServer.start_link(__MODULE__, args, opts)
  end

  def lookup(name, shard, key, opts \\ []) do
    [pid] = Registry.lookup(DatabaseRegistry, {name, shard})
    {duration, reply} = :timer.tc(GenServer, :call, [pid, {:lookup, key}, @call_timeout])
    backoff(duration, name, shard, opts)
    reply
  end

  def insert(name, shard, recs, opts \\ []) do
    [{pid, _value}] = Registry.lookup(DatabaseRegistry, {name, shard})
    {duration, reply} = :timer.tc(GenServer, :call, [pid, {:insert, recs}, @call_timeout])
    backoff(duration, name, shard, opts)
    reply
  end

  # gen_server callbacks

  @impl true
  def init(args) do
    # Process.flag(:trap_exit, true)

    db_path = args[:db_path]

    {:ok, db} = Sqlitex.open(db_path)
    # :ok = Sqlitex.exec(db,
    #   "CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));")
    {:ok, statement} = :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)

    state = %{
      db_path: db_path,
      db: db,
      statement: statement,
    }

    {:ok, state}
  end

  @impl true
  def terminate(reason, state) do
    %{db_path: db_path, db: db} = state
    Logger.info("Closing #{db_path} #{inspect(reason)}")
    :ok = :esqlite3.close(db)
  end

  @impl true
  def handle_call({:lookup, key}, _from, state) do
    db = state.db
    reply = Sqlitex.query(db, "SELECT value FROM kv_data where key = $1",
      bind: [key], into: %{})

    {:reply, reply, state}
  end

  def handle_call({:insert, recs}, _from, state) do
    reply = insert_db(recs, state, 1)
    {:reply, reply, state}
  end

  defp insert_db(recs, state, attempt) do
    %{db: db, statement: statement, db_path: db_path} = state
    try do
      with {:begin, :ok} <- {:begin, :esqlite3.exec("begin;", db)},
           {:insert, :ok} <- {:insert, insert_rows(statement, recs)},
           {:commit, :ok} <- {:commit, :esqlite3.exec("commit;", db)}
      do
        {:ok, attempt}
      else
        # {:prepare, {:error, {:busy, 'database is locked'}}}
        err ->
          Logger.warning("Error writing #{db_path} recs #{length(recs)} attempt #{attempt}: #{inspect(err)}")
          Process.sleep(100)
          insert_db(recs, state, attempt + 1)
      end
    catch
      {:error, :timeout, _ref} ->
        Logger.warning("Timeout writing #{db_path} recs #{length(recs)} attempt #{attempt}")
        insert_db(recs, state, attempt + 1)

      err ->
        Logger.error("Caught error #{db_path} recs #{length(recs)} attempt #{attempt} #{inspect(err)}")
        insert_db(recs, state, attempt + 1)
    end
  end

  def insert_rows(statement, recs) do
    for {key, value} <- recs, do: insert_row(statement, [key, value])

    # TODO: this just consumes errors
    :ok
  end

  defp insert_row(statement, params), do: insert_row(statement, params, :first, 1)

  defp insert_row(statement, params, :first, count) do
    :ok = :esqlite3.bind(statement, params)
    insert_row(statement, params, :esqlite3.step(statement), count)
  end

  defp insert_row(statement, params, :"$busy", count) do
    :timer.sleep(10)
    insert_row(statement, params, :esqlite3.step(statement), count + 1)
  end

  defp insert_row(_statement, _params, :"$done", count) do
    if count > 1 do
      Logger.debug("sqlite3 busy count: #{count}")
    end

    {:ok, count}
  end

  defp insert_row(_statement, params, {:error, reason}, _count) do
    Logger.error("esqlite: Error inserting #{inspect(params)}: #{inspect(reason)}")

    {:error, reason}
  end

  @spec backoff(atom(), pos_integer(), non_neg_integer(), Keyword.t()) :: true
  def backoff(name, shard, duration, opts \\ []) do
    # Duration threshold to trigger backoff (ms)
    threshold = opts[:backoff_threshold] || 400
    # Backoff is duration times multiple
    multiple = opts[:backoff_multiple] || 5
    # Maximum backoff
    max = opts[:backoff_max] || 5_000

    # duration is in microseconds, natively, convert to milliseconds
    duration = div(duration, 1024)
    if duration > threshold do
      backoff = min(duration * multiple, max)
      Logger.warning("#{name} #{shard} duration #{duration} backoff #{backoff}")
      Process.sleep(max)
    end

    true
  end

  # def find(name, shard, args) do
  #   id = {name, shard}
  #   case Registry.lookup(FileConfigSqlite.Registry, id) do
  #     [] ->
  #       {:ok, pid} = DynamicSupervisor.start_child(FileConfigSqlite.DatabaseManager, {FileConfigSqlite.DatabaseSupervisor, args})
  #       Logger.debug("Started database #{inspect(id)}: #{inspect(pid)}")
  #       {:ok, pid}

  #     [value] ->
  #       Logger.debug("Found database #{inspect(id)}: #{inspect(value)}")
  #       {:ok, value}
  #   end
  # end
end
