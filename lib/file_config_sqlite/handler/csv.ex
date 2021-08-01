defmodule FileConfigSqlite.Handler.Csv do
  @moduledoc "Handler for CSV files with sqlite backend"

  require Logger

  alias FileConfig.Loader
  alias FileConfig.Lib

  NimbleCSV.define(Module.concat(__MODULE__, Parser), separator: "\t", escape: "\e")
  alias FileConfigSqlite.Handler.Csv.Parser

  @spec init_config(map(), Keyword.t()) :: {:ok, map()} | {:error, term()}
  def init_config(config, args) do
    state_dir = args[:state_dir]

    db_name = config[:db_name] || config[:name]
    db_dir = Path.join(state_dir, to_string(db_name))
    :ok = File.mkdir_p(db_dir)

    shards = config[:shards] || 1

    for shard <- Range.new(1, shards) do
      db_path = Path.join(db_dir, "#{shard}.db")
      {:ok, _result} = create_db(db_path, config)
    end

    state_path = Path.join(db_dir, "state.json")

    handler_config = %{
      db_dir: db_dir,
      state_path: state_path,
      chunk_size: config[:chunk_size] || config[:commit_cycle] || 100,
      shards: shards,
    }

    {:ok, Map.merge(config, handler_config)}
  end

  @spec lookup(Loader.table_state(), term()) :: term()
  def lookup(%{id: tid, name: name, parser: parser} = state, key) do
    Logger.debug("table_state: #{inspect(state)}")
    parser_opts = state[:parser_opts] || []
    db_dir = state.db_dir
    shards = state.shards

    case :ets.lookup(tid, key) do
      [{_key, :undefined}] ->
        # Cached "not found" result
        :undefined

      [{_key, value}] ->
        # Cached result
        {:ok, value}

      [] ->
        # Not found
        shard = Lib.hash_to_bucket(key, shards)
        db_path = Path.join(db_dir, "#{shard}.db")
        {:ok, results} =
          Sqlitex.with_db(db_path,
              fn db ->
                Sqlitex.query(db,
                  "SELECT value FROM kv_data where key = $1",
                  bind: [key], into: %{})
              end)

        case results do
          [%{value: bin}] ->
            case parser.decode(bin, parser_opts) do
              {:ok, value} ->
                # Cache parsed value
                true = :ets.insert(tid, [{key, value}])
                {:ok, value}

              {:error, reason} ->
                Logger.debug("Error parsing table #{name} key #{key}: #{inspect(reason)}")
                {:ok, bin}
            end

          [] ->
            # Cache "not found" result
            true = :ets.insert(tid, [{key, :undefined}])
            :undefined
        end
    end
  end

  @spec load_update(Loader.name(), Loader.update(), :ets.tid(), Loader.update()) :: Loader.table_state()
  def load_update(name, update, tid, prev) do
    config = update.config

    Logger.debug("#{name} #{inspect(update)}")

    state_path = config[:state_path]
    state_mod = Lib.file_mtime(state_path)

    files =
      update
      |> Loader.changed_files?(prev)
      |> Loader.latest_file?()
      # Files stored latest first, process in chronological order
      |> Enum.reverse()
      # files = Enum.sort(update.files, fn({_, %{mod: a}}, {_, %{mod: b}}) -> a <= b end)

    # Logger.debug("files: #{inspect(files)}")
    # Logger.debug("state_mod: #{inspect(state_mod)}")
    # Logger.debug("update.mod: #{inspect(update.mod)}")

    if update.mod > state_mod do
      for {path, %{mod: file_mod}} <- files, file_mod > state_mod do
        Logger.info("Loading #{name} #{path} #{inspect(file_mod)}")
        {time, {:ok, rec}} = :timer.tc(&parse_file/2, [path, config])
        Logger.info("Loaded #{name} #{path} #{rec} rec #{time / 1_000_000} sec")
        # Record last successful file load
        :ok = File.touch(state_path, file_mod)
      end

      Logger.info("Loaded #{name} complete")
    else
      Logger.info("Loaded #{name} up to date")
    end

    table_state = Loader.make_table_state(__MODULE__, name, update, tid)
    state_config_keys = [:db_dir, :chunk_size, :commit_cycle, :shards]
    handler_state = Map.take(config, state_config_keys)
    # Map.put(table_state, :config, state_config)
    Map.merge(table_state, handler_state)
  end

  @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) :: true
  def insert_records(state, records) when is_list(records) do
    # true = :ets.insert(state.id, records)

    # {:ok, db} = Sqlitex.open(state.config.db_path)

    records
    |> Enum.sort()
    |> Enum.chunk_every(state.config.chunk_size)
    |> Enum.each(&write_chunk(&1, state.config))

    # :ok = :esqlite3.close(db)

    # Delete values in memory, causing them to be loaded again from disk
    for {key, _value} <- records do
      true = :ets.delete(state.id, key)
    end

    # Record time of last update
    :ok = File.touch(state.config.state_path)

    true
  end

  def insert_records(state, record) when is_tuple(record), do: insert_records(state, [record])

  # Internal functions

  @spec parse_file(Path.t(), map()) :: {:ok, non_neg_integer()}
  defp parse_file(path, config) do
    chunk_size = config[:chunk_size] || 100
    {key_field, value_field} = config[:csv_fields] || {1, 2}
    fetch_fn = Lib.make_fetch_fn(key_field, value_field)

    start_time = :os.timestamp()

    results =
      path
      |> File.stream!(read_ahead: 100_000)
      |> Parser.parse_stream(skip_headers: false)
      |> Stream.map(fetch_fn)
      |> Stream.chunk_every(chunk_size)
      # |> Stream.map(&write_chunk(&1, config))
      |> Task.async_stream(&write_chunk(&1, config), max_concurrency: System.schedulers_online() * 2, timeout: :infinity)
      |> Enum.map(fn {:ok, value} -> value end)

    # results = Enum.to_list(stream)

    {num_recs, _duration} =
      for {r, d} <- results, reduce: {0, 0} do
        {r_tot, d_tot} -> {r_tot + r, d_tot + d}
      end

    tprocess = :timer.now_diff(:os.timestamp(), start_time) / 1_000_000

    name = config.name
    # Logger.debug("Loaded #{name} recs #{num_recs} open #{topen / 1_000_000} process #{tprocess}")
    Logger.debug("Loaded #{name} recs #{num_recs} process #{tprocess}")

    {:ok, num_recs}
  end

  @spec parse_file_incremental(Path.t(), :ets.tab(), map()) :: {:ok, non_neg_integer()}
  def parse_file_incremental(path, _tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}
    commit_cycle = config[:commit_cycle] || 10000
    parser_processes = config[:parser_processes] || :erlang.system_info(:schedulers_online)

    db_path = config.db_path

    # {_tread, {:ok, bin}} = :timer.tc(File, :read, [path])
    # {tparse, r} = :timer.tc(:file_config_csv2, :pparse, [bin, :erlang.system_info(:schedulers_online), evt, 0])

    evt = fn
      # Called for each line
      {:line, line}, acc ->
        len = length(line)
        key = Lib.rnth(k, line, len)
        value = Lib.rnth(v, line, len)

        # Commit in the middle to avoid timeouts as write transactions wait for sync
        record_num = acc.record_num

        if rem(record_num, acc.cycle) == 0 do
          db = acc.db
          :esqlite3.exec("commit;", db)
          :esqlite3.exec("begin;", db)
        end

        statement = acc.statement
        :ok = insert_row(statement, [key, value])
        %{acc | record_num: record_num + 1}

      # Called before parsing shard
      {:shard, _shard}, acc ->
        {:ok, db} = Sqlitex.open(db_path)
        # {:ok, statement} = :esqlite3.prepare(
        #   """
        #   INSERT OR IGNORE INTO kv_data (key, value) VALUES(?1, ?2);
        #   UPDATE kv_data SET key = ?1, value = ?2 WHERE key = ?1 AND (Select Changes() = 0);
        #   """, db)
        {:ok, statement} =
          :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)

        :ok = :esqlite3.exec("begin;", db)

        Map.merge(acc, %{
          db: db,
          statement: statement,
          cycle: commit_cycle + :rand.uniform(commit_cycle)
        })

      # Called after parsing shard
      :eof, acc ->
        db = acc.db
        :ok = :esqlite3.exec("commit;", db)
        :ok = :esqlite3.close(db)
        acc
    end

    {:ok, bin} = File.read(path)
    r = :file_config_csv2.pparse(bin, parser_processes, evt, %{record_num: 0})
    num_records = Enum.reduce(r, 0, fn x, a -> a + x.record_num end)
    {:ok, num_records}
  end

  @spec write_chunk(list({term(), term()}), map()) :: {non_neg_integer(), non_neg_integer()}
  defp write_chunk(recs, config) do
    start_time = :os.timestamp()

    shard_recs =
      Enum.group_by(recs,
        fn [key, _value] ->
          Lib.hash_to_bucket(key, config.shards)
        end)

    for {shard, recs} <- shard_recs do
      Logger.debug("shard #{shard} recs: #{inspect(length(recs))}")
      db_path = Path.join(config.db_dir, "#{shard}.db")
      write_db(recs, db_path, 1)
    end

    duration = :timer.now_diff(:os.timestamp(), start_time)

    {length(recs), duration}
  end

  defp write_db(recs, db_path, attempt) do
    with {:open, {:ok, db}} <- {:open, Sqlitex.open(db_path)},
         {:prepare, {:ok, statement}} <-
           {:prepare, :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)},
         {:begin, :ok} <- {:begin, :esqlite3.exec("begin;", db)},
         {:insert, :ok} <- {:insert, insert_rows(statement, recs)},
         {:commit, :ok} <- {:commit, :esqlite3.exec("commit;", db)},
         {:close, :ok} <- {:close, :esqlite3.close(db)}
    do
      {:ok, attempt}
    else
      # {:error, {:busy, 'database is locked'}}
      err ->
        Logger.warning("Error writing #{db_path} recs #{length(recs)} attempt #{attempt}: #{inspect(err)}")
        Process.sleep(100)
        write_db(recs, db_path, attempt + 1)
    end
  end

  def insert_rows(statement, recs) do
    for params <- recs, do: insert_row(statement, params)

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

    :ok
  end

  defp insert_row(_statement, params, {:error, reason}, _count) do
    Logger.error("esqlite: Error inserting #{inspect(params)}: #{inspect(reason)}")
    :ok
  end

  @spec create_db(Path.t(), map()) :: {:ok, term()} | Sqlitex.sqlite_error()
  defp create_db(db_path, _config) do
    Logger.debug("Creating db #{db_path}")

    Sqlitex.with_db(db_path, fn db ->
      # TODO: make field sizes configurable
      Sqlitex.query(
        db,
        "CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));"
      )
    end)
  end

  # defp commit(db) do
  #   try do
  #     :ok = :esqlite3.exec("commit;", db)
  #   catch
  #     {:error, :timeout, _ref} ->
  #       Logger.warning("sqlite3 timeout")
  #       commit(db)
  #     error ->
  #       Logger.warning("sqlite3 error #{inspect error}")
  #   end
  # end
end
