defmodule FileConfigSqlite.Database do
  @moduledoc ~S"""
  Top level API for database operations and shard database handler.

  """
  use GenServer

  require Logger

  alias FileConfigSqlite.DatabaseRegistry

  # @call_timeout 30_000
  @call_timeout 5_000
  # @call_timeout :infinity

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
    [{pid, _value}] = Registry.lookup(DatabaseRegistry, {name, shard})
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

    {:ok, db, insert_statement, select_statement} = open_db(db_path)

    state = %{
      db_path: db_path,
      db: db,
      insert_statement: insert_statement,
      select_statement: select_statement,
    }

    {:ok, state}
  end

  @impl true
  def terminate(reason, state) do
    %{db_path: db_path, db: db} = state
    Logger.info("Closing #{db_path} #{inspect(reason)}")
    close_db(db)
  end

  def open_db(db_path) do
    with {:open, {:ok, db}} <- {:open, Exqlite.Sqlite3.open(db_path)},
         {:create_table, :ok} <- {:create_table, Exqlite.Sqlite3.execute(db,
           "CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));")},
         {:pragma, :ok} <- {:pragma, Exqlite.Sqlite3.execute(db, "PRAGMA journal_mode = MEMORY;")},
         {:prepare, {:ok, insert_statement}} <- {:prepare, Exqlite.Sqlite3.prepare(db, "INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);")},
         {:prepare, {:ok, select_statement}} <- {:prepare, Exqlite.Sqlite3.prepare(db, "SELECT value FROM kv_data where key = ?1")}
    do
      {:ok, db, insert_statement, select_statement}
    else
      {_step, reason} = err ->
        Logger.error("Error opening db: #{inspect(err)}")
        reason
    end
    # {:ok, db} = Exqlite.Sqlite3.open(db_path)
    # :ok = Exqlite.Sqlite3.execute(db,
    #   "CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));")
    # :ok = Exqlite.Sqlite3.execute(db, "PRAGMA journal_mode = MEMORY;")
    # {:ok, insert_statement} = Exqlite.Sqlite3.prepare(db, "INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);")
    # {:ok, select_statement} = Exqlite.Sqlite3.prepare(db, "SELECT value FROM kv_data where key = ?1")

    # {:ok, db} = Sqlitex.open(db_path)
    # :ok = Sqlitex.exec(db,
    #   "CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));")
    # # :ok = Sqlitex.exec(db, "PRAGMA journal_mode = WAL;")
    # :ok = Sqlitex.exec(db, "PRAGMA journal_mode = MEMORY;")
    # {:ok, statement} = :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)
  end

  def close_db(db) do
    Exqlite.Sqlite3.close(db)
    # :ok = :esqlite3.close(db)
  end

  def fetch_db(db, statement) do
    case Exqlite.Sqlite3.fetch_all(db, statement) do
      {:ok, rows} ->
        rows = for [value] <- rows, do: %{value: value}
        {:ok, rows}
      err ->
        err
    end
    # reply = Sqlitex.query(db, "SELECT value FROM kv_data where key = $1",
    #   bind: [key], into: %{})
  end

  @impl true
  def handle_call({:lookup, key}, _from, state) do
    db = state.db

    with {:ok, statement} <- Exqlite.Sqlite3.prepare(db,
            "SELECT value FROM kv_data where key = ?1"),
         :ok <- Exqlite.Sqlite3.bind(db, statement, [key])
    do
      reply = fetch_db(db, statement)
      {:reply, reply, state}
    else
      {:error, reason} = reply ->
        Logger.error("SQL error: #{inspect(reason)}")
      {:reply, reply, state}
    end
  end

  def handle_call({:insert, recs}, _from, state) do
    %{db: db, insert_statement: statement, db_path: db_path} = state
    reply = insert_db(db, statement, recs, db_path, 1)
    {:reply, reply, state}
  end

  def insert_db(db, statement, recs, db_path, attempt) do
    try do
      with {:begin, :ok} <- {:begin,  Exqlite.Sqlite3.execute(db, "begin;")},
           {:insert, :ok} <- {:insert, insert_rows(db, statement, recs)},
           {:commit, :ok} <- {:commit,  Exqlite.Sqlite3.execute(db, "commit;")}
      do
        {:ok, attempt}
      else
        err ->
          Logger.warning("Error #{db_path} recs #{length(recs)} attempt #{attempt}: #{inspect(err)}")
          insert_db(db, statement, recs, db_path, attempt + 1)
      end

      # case insert_rows(statement, recs) do
      #   :ok ->
      #     :ok
      #   err ->
      #     Logger.warning("Error #{db_path} recs #{length(recs)} attempt #{attempt}: #{inspect(err)}")
      #     insert_db(recs, state, attempt + 1)
      # end

      # with {:begin, :ok} <- {:begin, :esqlite3.exec("begin;", db)},
      #      {:insert, :ok} <- {:insert, insert_rows(statement, recs)},
      #      {:commit, :ok} <- {:commit, :esqlite3.exec("commit;", db)}
      # do
      #   {:ok, attempt}
      # else
      #   # {:prepare, {:error, {:busy, 'database is locked'}}}
      #   err ->
      #     Logger.warning("Error writing #{db_path} recs #{length(recs)} attempt #{attempt}: #{inspect(err)}") Process.sleep(100)
      #     insert_db(recs, state, attempt + 1)
      # end
    catch
      {:error, :timeout, _ref} ->
        Logger.warning("Timeout writing #{db_path} recs #{length(recs)} attempt #{attempt}")
        insert_db(db, statement, recs, db_path, attempt + 1)

      err ->
        Logger.error("Caught error #{db_path} recs #{length(recs)} attempt #{attempt} #{inspect(err)}")
        insert_db(db, statement, recs, db_path, attempt + 1)
    end
  end

  def query_result(db, statement, result)

  def query_result(_db, _statement, {:row, []}), do: {:ok, []}

  def query_result(_db, _statement, {:row, [value]}), do: {:ok, [%{value: value}]}

  def query_result(db, statement, :busy) do
    query_result(db, statement, Exqlite.Sqlite3.step(db, statement))
  end

  def insert_rows(db, statement, recs) do
    for {key, value} <- recs, do: insert_row(db, statement, [key, value])

    # TODO: this just consumes errors
    :ok
  end

  defp insert_row(db, statement, params), do: insert_row(db, statement, params, :first, 1)

  defp insert_row(db, statement, params, :first, count) do
    :ok = Exqlite.Sqlite3.bind(db, statement, params)
    insert_row(db, statement, params, Exqlite.Sqlite3.step(db, statement), count)
  end

  defp insert_row(db, statement, params, :busy, count) do
    :timer.sleep(100)
    insert_row(db, statement, params, Exqlite.Sqlite3.step(db, statement), count + 1)
  end

  defp insert_row(_db, _statement, _params, :done, count) do
    if count > 1 do
      Logger.debug("sqlite3 busy count: #{count}")
    end
    {:ok, count}
  end

  defp insert_row(_db, _statement, params, {:error, reason}, _count) do
    Logger.error("Error inserting #{inspect(params)}: #{inspect(reason)}")
    {:error, reason}
  end


  # defp insert_row(statement, params), do: insert_row(statement, params, :first, 1)

  # defp insert_row(statement, params, :first, count) do
  #   :ok = :esqlite3.bind(statement, params)
  #   insert_row(statement, params, :esqlite3.step(statement), count)
  # end

  # defp insert_row(statement, params, :"$busy", count) do
  #   :timer.sleep(10)
  #   insert_row(statement, params, :esqlite3.step(statement), count + 1)
  # end

  # defp insert_row(_statement, _params, :"$done", count) do
  #   if count > 1 do
  #     Logger.debug("sqlite3 busy count: #{count}")
  #   end
  #   {:ok, count}
  # end

  # defp insert_row(_statement, params, {:error, reason}, _count) do
  #   Logger.error("esqlite: Error inserting #{inspect(params)}: #{inspect(reason)}")
  #   {:error, reason}
  # end

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
