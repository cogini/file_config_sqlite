defmodule FileConfigSqlite.Handler.Csv do
  @moduledoc "Handler for CSV files with sqlite backend"

  alias FileConfig.Lib
  alias FileConfig.Loader
  alias FileConfigSqlite.Database
  alias FileConfigSqlite.DatabaseManager
  alias FileConfigSqlite.DatabaseRegistry
  alias FileConfigSqlite.DatabaseSupervisor

  require Logger

  NimbleCSV.define(FileConfigSqlite.Handler.Csv.Parser, separator: "\t", escape: "\e")

  # @type reason :: FileConfig.reason()
  @type reason :: atom() | binary()

  @spec init_config(map(), Keyword.t()) :: {:ok, map()} | {:error, reason()}
  def init_config(config, args) do
    state_dir = args[:state_dir]

    db_name = config[:db_name] || config[:name]
    db_dir = Path.join(state_dir, to_string(db_name))
    :ok = File.mkdir_p(db_dir)

    shards = config[:shards] || 1

    for shard <- Range.new(1, shards) do
      db_path = Path.join(db_dir, "#{shard}.db")

      {:ok, _pid} = start_database(name: db_name, shard: shard, db_path: db_path)
    end

    state_path = Path.join(db_dir, "state.json")

    handler_config = %{
      db_dir: db_dir,
      state_path: state_path,
      chunk_size: config[:chunk_size] || config[:commit_cycle] || 100,
      shards: shards
    }

    {:ok, Map.merge(config, handler_config)}
  end

  @spec read(Loader.table_state(), term()) :: {:ok, term()} | nil | {:error, reason()}
  def read(%{id: tab, name: name, parser: parser} = state, key) do
    parser_opts = state[:parser_opts] || []
    shards = state.shards

    case :ets.lookup(tab, key) do
      [{_key, :undefined}] ->
        # Cached "not found" result
        nil

      [{_key, value}] ->
        # Cached result
        {:ok, value}

      [] ->
        # Not found
        shard = Lib.hash_to_bucket(key, shards)
        {:ok, results} = Database.lookup(name, shard, key)

        case results do
          [%{value: bin}] ->
            case parser.decode(bin, parser_opts) do
              {:ok, value} ->
                # Cache parsed value
                true = :ets.insert(tab, [{key, value}])
                {:ok, value}

              {:error, reason} ->
                {:error, {:parse, bin, reason}}
                # Logger.debug("Error parsing value for table #{name} key #{key}: #{inspect(reason)}")
                # {:ok, bin}
            end

          [] ->
            # Cache "not found" result
            true = :ets.insert(tab, [{key, :undefined}])
            nil
        end
    end
  end

  @deprecated "Use read/2 instead"
  @spec lookup(Loader.table_state(), term()) :: term()
  def lookup(state, key) do
    case read(state, key) do
      {:ok, value} ->
        value

      nil ->
        :undefined
    end
  end

  @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) ::
          :ok | {:error, reason()}
  # @spec insert_records(Loader.table_state(), {term(), term()} | [{term(), term()}]) :: true
  def insert_records(state, record) when is_tuple(record), do: insert_records(state, [record])

  def insert_records(state, records) when is_list(records) do
    records
    # |> Enum.sort()
    |> Enum.with_index(1)
    |> Enum.chunk_every(state.chunk_size)
    |> Enum.each(&write_chunk(&1, state))

    # Delete values from cache, causing them to be loaded again on next request
    for {key, _value} <- records do
      true = :ets.delete(state.id, key)
    end

    :ok
  end

  @spec load_update(Loader.name(), Loader.update(), :ets.tab(), Loader.update()) ::
          Loader.table_state()
  def load_update(name, update, tab, prev) do
    config = update.config
    state_path = config.state_path

    files =
      update
      |> Loader.changed_files?(prev)
      |> Loader.latest_file?()
      # Files stored latest first, process in chronological order
      |> Enum.reverse()

    state_mod = Lib.file_mtime(state_path)

    if update.mod > state_mod do
      start_time = :os.timestamp()

      for {path, %{mod: file_mod}} <- files, file_mod > state_mod do
        Logger.info("Loading #{name} #{path} #{inspect(file_mod)}")

        {time, {:ok, num_recs}} = :timer.tc(&parse_file/2, [path, config])

        Logger.info("Loaded #{name} #{path} #{num_recs} rec #{time / 1_000_000} sec")

        # Record last successful file load
        :ok = File.touch(state_path, file_mod)
      end

      duration = :timer.now_diff(:os.timestamp(), start_time) / 1_000_000

      Logger.info("Loaded #{name} complete, loaded #{length(files)} files in #{duration} sec")
    else
      Logger.info("Loaded #{name} up to date")
    end

    table_state = Loader.make_table_state(__MODULE__, name, update, tab)
    handler_state = Map.take(config, [:db_dir, :chunk_size, :commit_cycle, :shards])
    Map.merge(table_state, handler_state)
  end

  # Internal functions

  @spec start_database(Keyword.t()) :: Supervisor.on_start_child()
  def start_database(args) do
    name = args[:name]
    shard = args[:shard]
    db_path = args[:db_path]

    case Registry.lookup(DatabaseRegistry, {name, shard}) do
      [] ->
        {:ok, pid} =
          DynamicSupervisor.start_child(DatabaseManager, {DatabaseSupervisor, args})

        Logger.info("Started database #{db_path} #{inspect(pid)}")
        {:ok, pid}

      [{pid, _value}] ->
        Logger.info("Found database #{db_path} #{inspect(pid)}")
        {:ok, pid}
    end
  end

  # Get stream which parses input file
  def parse_file_stream(path, config) do
    {key_field, value_field} = config[:csv_fields] || {1, 2}
    fetch_fn = Lib.make_fetch_fn(key_field, value_field)

    path
    |> File.stream!(read_ahead: 10_000_000)
    |> FileConfigSqlite.Handler.Csv.Parser.parse_stream(skip_headers: false)
    |> Stream.map(fetch_fn)
    # |> Stream.map(fn [key, value] -> {key, value} end)
    |> Stream.map(fn [key, value] -> {:binary.copy(key), :binary.copy(value)} end)
    |> Stream.with_index(1)
  end

  def parse_csv_file(path, config) do
    {key_field, value_field} = config[:csv_fields] || {1, 2}
    fetch_fn = Lib.make_fetch_fn(key_field, value_field)

    path
    |> File.read!()
    |> FileConfigSqlite.Handler.Csv.Parser.parse_stream(skip_headers: false)
    |> Enum.map(fetch_fn)
    # |> Enum.map(fn [key, value] -> {key, value} end)
    |> Stream.map(fn [key, value] -> {:binary.copy(key), :binary.copy(value)} end)
    |> Enum.with_index(1)
  end

  # Unpack index and report progress
  def unpack_index(recs, path, config) do
    report_count = config[:report_count]

    for {{key, _value} = rec, index} <- recs do
      if report_count do
        if rem(index, report_count) == 0 do
          Logger.info("Processing #{config.name} #{path} rec #{index} #{key}")
        end
      end

      rec
    end
  end

  defp shard_recs(recs, path, config) do
    recs
    |> unpack_index(path, config)
    # Group by shard
    |> Enum.group_by(fn {key, _value} -> Lib.hash_to_bucket(key, config.shards) end)
  end

  @spec parse_file(Path.t(), map()) :: {:ok, non_neg_integer()}
  defp parse_file(path, config) do
    chunk_size = config[:chunk_size] || 100
    name = config.name

    start_time = :os.timestamp()

    shard_recs =
      path
      # |> parse_file_stream(config)
      |> parse_csv_file(config)
      |> shard_recs(path, config)

    results =
      for {shard, recs} <- shard_recs do
        recs
        |> Enum.chunk_every(chunk_size)
        # |> Enum.map(&do_insert(name, shard, &1))
        |> then(fn chunks -> insert_shard_chunks(chunks, name, shard, path, config) end)
      end

    # |> Stream.map(&write_chunk(&1, config))
    # This is faster on SSD, but overloads the HDD
    # |> Task.async_stream(&write_chunk(&1, config), max_concurrency: System.schedulers_online() * 2, timeout: :infinity)
    # |> Enum.map(fn {:ok, value} -> value end)

    {num_recs, _duration} =
      for {r, d} <- List.flatten(results), reduce: {0, 0} do
        {r_tot, d_tot} -> {r_tot + r, d_tot + d}
      end

    tprocess = :timer.now_diff(:os.timestamp(), start_time) / 1_000_000

    Logger.debug("Loaded #{name} recs #{num_recs} process #{tprocess} s")

    {:ok, num_recs}
  end

  @spec parse_file_incremental(Path.t(), :ets.tab(), map()) :: {:ok, non_neg_integer()}
  def parse_file_incremental(path, _tid, config) do
    {k, v} = config[:csv_fields] || {1, 2}
    commit_cycle = config[:commit_cycle] || 10_000
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
          :ok = :esqlite3.exec(db, "commit;")
          :ok = :esqlite3.exec(db, "begin;")
        end

        statement = acc.statement
        :ok = insert_row(statement, [key, value])
        %{acc | record_num: record_num + 1}

      # Called before parsing shard
      {:shard, _shard}, acc ->
        # {:ok, db} = Sqlitex.open(db_path)
        {:ok, db} = :esqlite3.open(db_path)
        # {:ok, statement} = :esqlite3.prepare(
        #   """
        #   INSERT OR IGNORE INTO kv_data (key, value) VALUES(?1, ?2);
        #   UPDATE kv_data SET key = ?1, value = ?2 WHERE key = ?1 AND (Select Changes() = 0);
        #   """, db)
        {:ok, statement} =
          :esqlite3.prepare(db, "INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", [])

        :ok = :esqlite3.exec(db, "begin;")

        Map.merge(acc, %{
          db: db,
          statement: statement,
          cycle: commit_cycle + :rand.uniform(commit_cycle)
        })

      # Called after parsing shard
      :eof, acc ->
        db = acc.db
        :ok = :esqlite3.exec(db, "commit;")
        :ok = :esqlite3.close(db)
        acc
    end

    {:ok, bin} = File.read(path)
    r = :file_config_csv2.pparse(bin, parser_processes, evt, %{record_num: 0})
    num_records = Enum.reduce(r, 0, fn x, a -> a + x.record_num end)
    {:ok, num_records}
  end

  @spec write_chunk(list(tuple()), map()) :: {non_neg_integer(), non_neg_integer()}
  defp write_chunk(recs, config) do
    name = config.name
    report_count = config[:report_count]

    start_time = :os.timestamp()

    shard_recs =
      recs
      # Unpack index and report progress
      |> Enum.map(fn {{key, _value} = record, index} ->
        if report_count do
          if rem(index, report_count) == 0 do
            Logger.info("Processing #{config.name} record #{index} #{key}")
          end
        end

        record
      end)
      # Group by shard
      |> Enum.group_by(fn {key, _value} -> Lib.hash_to_bucket(key, config.shards) end)

    for {shard, recs} <- shard_recs do
      # Logger.debug("shard #{shard} recs: #{inspect(length(recs))}")
      # db_path = Path.join(config.db_dir, "#{shard}.db")
      # {time, result} = :timer.tc(&write_db/3, [recs, db_path, 1])
      # {time, result} = :timer.tc(Database, :insert, [name, shard, recs])
      {time, result} = do_insert(name, shard, recs)

      Logger.info("Inserted #{config.name} shard #{shard} #{length(recs)} rec in #{time / 1_000_000} s")

      result
    end

    duration = :timer.now_diff(:os.timestamp(), start_time)

    {length(recs), duration}
  end

  def insert_shard_chunks(chunks, name, shard, path, config) do
    db_dir = config[:db_dir]
    db_path = Path.join(db_dir, "#{shard}.db")

    {:ok, db, statement, _select_statement} = Database.open_db(db_path)

    results =
      for recs <- chunks do
        start_time = :os.timestamp()

        {:ok, attempts} = Database.insert_db(db, statement, recs, db_path, 1)

        duration = :timer.now_diff(:os.timestamp(), start_time)

        Logger.info(
          "Insert db #{name} #{shard} #{path} #{length(recs)} rec #{duration / 1_000_000} s attempts #{attempts}"
        )

        {length(recs), duration}
      end

    :ok = Database.close_db(db)

    results
  end

  # defp insert_db(db, statement, recs, db_path, attempt) do

  #   {time, result} = :timer.tc(Database, :insert, [name, shard, recs])
  #   Logger.info("inserted #{name} #{shard} #{length(recs)} recs in #{time / 1_000_000} s")
  #   {time, result}
  # catch
  #   :exit, {:timeout, _reason} ->
  #     Logger.error("catch exit #{name} #{shard} timeout")
  #     do_insert(name, shard, recs)
  #   :exit, _reason ->
  #     # Logger.error("catch exit #{name} #{shard} #{inspect(reason)}")
  #     Logger.error("catch exit #{name} #{shard}")
  #     do_insert(name, shard, recs)
  #   err ->
  #     Logger.error("catch #{name} #{shard} #{inspect(err)}")
  #     do_insert(name, shard, recs)
  # end

  def do_insert(name, shard, recs) do
    # Logger.info("Inserting #{name} #{shard} #{length(recs)}")
    {time, result} = :timer.tc(Database, :insert, [name, shard, recs])
    # Logger.debug("inserted #{name} #{shard} #{length(recs)} recs in #{time / 1_000_000} s")
    {time, result}
  catch
    :exit, {:timeout, _reason} ->
      Logger.error("catch exit #{name} #{shard} timeout")
      do_insert(name, shard, recs)

    :exit, _reason ->
      # Logger.error("catch exit #{name} #{shard} #{inspect(reason)}")
      Logger.error("catch exit #{name} #{shard}")
      do_insert(name, shard, recs)

    err ->
      Logger.error("catch #{name} #{shard} #{inspect(err)}")
      do_insert(name, shard, recs)
  end

  # defp write_db(recs, db_path, attempt) do
  #   try do
  #     with {:open, {:ok, db}} <- {:open, :esqlite3.open(db_path)},
  #          {:prepare, {:ok, statement}} <- {:prepare,
  #            :esqlite3.prepare("INSERT OR REPLACE INTO kv_data (key, value) VALUES(?1, ?2);", db)},
  #          {:begin, :ok} <- {:begin, :esqlite3.exec(db, "begin;")},
  #          {:insert, :ok} <- {:insert, insert_rows(statement, recs)},
  #          {:commit, :ok} <- {:commit, :esqlite3.exec(db, "commit;")},
  #          {:close, :ok} <- {:close, :esqlite3.close(db)}
  #     do
  #       {:ok, attempt}
  #     else
  #       # {:prepare, {:error, {:busy, 'database is locked'}}}
  #       err ->
  #         Logger.warning("Error writing #{db_path} recs #{length(recs)} attempt #{attempt}: #{inspect(err)}")
  #         Process.sleep(100)
  #         write_db(recs, db_path, attempt + 1)
  #     end
  #   catch
  #     {:error, :timeout, _ref} ->
  #       Logger.warning("timeout writing #{db_path} recs #{length(recs)} attempt #{attempt}")
  #       write_db(recs, db_path, attempt + 1)
  #
  #     err ->
  #       Logger.error("caught error #{db_path} recs #{length(recs)} attempt #{attempt} #{inspect(err)}")
  #       write_db(recs, db_path, attempt + 1)
  #   end
  # end

  def insert_rows(statement, recs) do
    for params <- recs, do: insert_row(statement, params)
    :ok
  end

  defp insert_row(statement, params), do: insert_row(statement, params, :first, 1)

  defp insert_row(statement, params, :first, count) do
    :ok = :esqlite3.bind(statement, params)
    insert_row(statement, params, :esqlite3.step(statement), count)
  end

  # defp insert_row(statement, params, :"$busy", count) do
  #   :timer.sleep(10)
  #   insert_row(statement, params, :esqlite3.step(statement), count + 1)
  # end

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

  # @spec create_db(Path.t(), map()) :: {:ok, term()} | Sqlitex.sqlite_error()
  # defp create_db(db_path, _config) do
  #   Logger.debug("Creating db #{db_path}")
  #
  #   Sqlitex.with_db(db_path, fn db ->
  #     # TODO: make field sizes configurable
  #     Sqlitex.query(
  #       db,
  #       "CREATE TABLE IF NOT EXISTS kv_data(key VARCHAR(64) PRIMARY KEY, value VARCHAR(1000));"
  #     )
  #   end)
  # end

  # defp commit(db) do
  #   try do
  #     :ok = :esqlite3.exec(db, "commit;")
  #   catch
  #     {:error, :timeout, _ref} ->
  #       Logger.warning("sqlite3 timeout")
  #       commit(db)
  #     error ->
  #       Logger.warning("sqlite3 error #{inspect error}")
  #   end
  # end
end
