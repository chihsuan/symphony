defmodule SymphonyElixir.RunStore do
  @moduledoc """
  Durable store for orchestrator run history, retry queue entries, and totals.
  """

  use GenServer
  require Logger

  alias SymphonyElixir.LogFile

  @runs_table :symphony_run_store_runs
  @retry_table :symphony_run_store_retries
  @totals_table :symphony_run_store_totals
  @tables [
    {@runs_table, [:run_id, :record]},
    {@retry_table, [:issue_id, :record]},
    {@totals_table, [:key, :record]}
  ]
  @data_tables [@runs_table, @retry_table, @totals_table]
  @codex_totals_key :codex_totals

  defmodule State do
    @moduledoc false

    defstruct [:dir]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec ensure_started() :: :ok | {:error, term()}
  def ensure_started do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        :ok

      _ ->
        case start_link([]) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @spec put_run(map()) :: :ok | {:error, term()}
  def put_run(%{run_id: run_id} = record) when is_binary(run_id) do
    with :ok <- ensure_started() do
      durable_transaction(fn ->
        :mnesia.write({@runs_table, run_id, normalize_record(record)})
        :ok
      end)
    end
  end

  def put_run(_record), do: {:error, :invalid_run_record}

  @spec update_run(String.t(), map()) :: :ok | {:error, term()}
  def update_run(run_id, attrs) when is_binary(run_id) and is_map(attrs) do
    with :ok <- ensure_started() do
      update_run_record(run_id, attrs)
      |> unwrap_nested_error()
    end
  end

  def update_run(_run_id, _attrs), do: {:error, :invalid_run_record}

  @spec list_runs() :: [map()] | {:error, term()}
  def list_runs, do: list_runs(50)

  @spec list_runs(non_neg_integer() | :all) :: [map()] | {:error, term()}
  def list_runs(limit) when is_integer(limit) and limit >= 0 do
    case list_runs(:all) do
      runs when is_list(runs) -> Enum.take(runs, limit)
      {:error, reason} -> {:error, reason}
    end
  end

  def list_runs(:all) do
    with :ok <- ensure_started() do
      transaction(fn ->
        @runs_table
        |> all_records()
        |> Enum.sort_by(&datetime_sort_key(Map.get(&1, :started_at)), :desc)
      end)
    end
  end

  @spec interrupt_running_runs(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def interrupt_running_runs(error) when is_binary(error) do
    now = DateTime.utc_now()

    with :ok <- ensure_started() do
      durable_transaction(fn ->
        {:ok, interrupt_running_records(error, now)}
      end)
      |> unwrap_nested_error()
    end
  end

  def interrupt_running_runs(_error), do: {:error, :invalid_error}

  @spec put_retry(map()) :: :ok | {:error, term()}
  def put_retry(%{issue_id: issue_id} = record) when is_binary(issue_id) do
    with :ok <- ensure_started() do
      durable_transaction(fn ->
        :mnesia.write({@retry_table, issue_id, normalize_record(record)})
        :ok
      end)
    end
  end

  def put_retry(_record), do: {:error, :invalid_retry_record}

  @spec delete_retry(String.t()) :: :ok | {:error, term()}
  def delete_retry(issue_id) when is_binary(issue_id) do
    with :ok <- ensure_started() do
      durable_transaction(fn ->
        :mnesia.delete({@retry_table, issue_id})
        :ok
      end)
    end
  end

  def delete_retry(_issue_id), do: {:error, :invalid_issue_id}

  @spec list_retries() :: [map()] | {:error, term()}
  def list_retries do
    with :ok <- ensure_started() do
      transaction(fn ->
        @retry_table
        |> all_records()
        |> Enum.sort_by(&datetime_sort_key(Map.get(&1, :due_at)), :asc)
      end)
    end
  end

  @spec put_codex_totals(map()) :: :ok | {:error, term()}
  def put_codex_totals(totals) when is_map(totals) do
    with :ok <- ensure_started() do
      durable_transaction(fn ->
        :mnesia.write({@totals_table, @codex_totals_key, normalize_record(totals)})
        :ok
      end)
    end
  end

  def put_codex_totals(_totals), do: {:error, :invalid_codex_totals}

  @spec get_codex_totals() :: map() | nil | {:error, term()}
  def get_codex_totals do
    with :ok <- ensure_started() do
      transaction(&read_codex_totals/0)
    end
  end

  @spec clear() :: :ok | {:error, term()}
  def clear do
    with :ok <- ensure_started() do
      @data_tables
      |> Enum.reduce_while(:ok, &clear_table/2)
      |> sync_after_clear()
    end
  end

  @impl true
  def init(opts) do
    dir = Keyword.get(opts, :dir, store_dir())

    case setup_mnesia(dir) do
      :ok ->
        {:ok, %State{dir: dir}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @spec store_dir() :: Path.t()
  def store_dir do
    Application.get_env(:symphony_elixir, :run_store_dir) ||
      Path.join(
        Path.dirname(Application.get_env(:symphony_elixir, :log_file, LogFile.default_log_file())),
        "run_store"
      )
  end

  defp setup_mnesia(dir) when is_binary(dir) do
    expanded_dir = Path.expand(dir)

    case File.mkdir_p(expanded_dir) do
      :ok -> start_and_ensure_mnesia(expanded_dir)
      {:error, reason} -> {:error, reason}
    end
  end

  defp setup_mnesia(_dir), do: {:error, :invalid_run_store_dir}

  defp start_and_ensure_mnesia(dir) do
    case start_mnesia(dir) do
      :ok -> ensure_tables()
      {:error, reason} -> {:error, reason}
    end
  end

  defp start_mnesia(dir) do
    case load_mnesia() do
      :ok -> start_or_validate_mnesia(dir)
      {:error, reason} -> {:error, reason}
    end
  end

  defp start_or_validate_mnesia(dir) do
    if mnesia_running?() do
      ensure_running_mnesia_dir(dir)
    else
      start_stopped_mnesia(dir)
    end
  end

  defp start_stopped_mnesia(dir) do
    Application.put_env(:mnesia, :dir, String.to_charlist(dir))

    case create_schema() do
      :ok -> normalize_mnesia_start(:mnesia.start())
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_mnesia do
    case Application.load(:mnesia) do
      :ok -> :ok
      {:error, {:already_loaded, :mnesia}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp mnesia_running? do
    :mnesia.system_info(:is_running) == :yes
  catch
    :exit, _reason -> false
  end

  defp create_schema do
    case :mnesia.create_schema([node()]) do
      :ok -> :ok
      {:error, {_, {:already_exists, _}}} -> :ok
      {:error, {:already_exists, _}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_mnesia_start(:ok), do: :ok
  defp normalize_mnesia_start({:error, reason}), do: {:error, reason}

  defp ensure_running_mnesia_dir(expected_dir) do
    running_dir =
      :mnesia.system_info(:directory)
      |> to_string()
      |> Path.expand()

    if running_dir == expected_dir do
      :ok
    else
      {:error, {:mnesia_dir_mismatch, %{expected: expected_dir, running: running_dir}}}
    end
  end

  defp ensure_tables do
    with :ok <- Enum.reduce_while(@tables, :ok, &ensure_table/2) do
      wait_for_tables()
    end
  end

  defp ensure_table({table, attributes}, :ok) do
    if table in :mnesia.system_info(:tables) do
      {:cont, :ok}
    else
      case :mnesia.create_table(table,
             attributes: attributes,
             disc_copies: [node()],
             type: :set
           ) do
        {:atomic, :ok} -> {:cont, :ok}
        {:aborted, {:already_exists, ^table}} -> {:cont, :ok}
        {:aborted, reason} -> {:halt, {:error, reason}}
      end
    end
  end

  defp wait_for_tables do
    case :mnesia.wait_for_tables(@data_tables, 5_000) do
      :ok -> :ok
      {:timeout, tables} -> {:error, {:mnesia_table_timeout, tables}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp transaction(fun) when is_function(fun, 0) do
    case :mnesia.transaction(fun) do
      {:atomic, result} -> result
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp update_run_record(run_id, attrs) do
    durable_transaction(fn ->
      case :mnesia.read(@runs_table, run_id) do
        [{@runs_table, ^run_id, record}] ->
          :mnesia.write({@runs_table, run_id, Map.merge(record, normalize_record(attrs))})
          :ok

        [] ->
          {:error, :run_not_found}
      end
    end)
  end

  defp durable_transaction(fun) when is_function(fun, 0) do
    case transaction(fun) do
      {:error, _reason} = error ->
        error

      result ->
        case sync_mnesia_log() do
          :ok ->
            result

          {:error, reason} ->
            Logger.warning("Run store transaction committed but failed to sync Mnesia log: #{inspect(reason)}")
            result
        end
    end
  end

  defp sync_mnesia_log do
    case :mnesia.sync_log() do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp clear_table(table, :ok) do
    case :mnesia.clear_table(table) do
      {:atomic, :ok} -> {:cont, :ok}
      {:aborted, reason} -> {:halt, {:error, reason}}
    end
  end

  defp sync_after_clear(:ok), do: sync_mnesia_log()
  defp sync_after_clear({:error, reason}), do: {:error, reason}

  defp read_codex_totals do
    case :mnesia.read(@totals_table, @codex_totals_key) do
      [{@totals_table, @codex_totals_key, totals}] -> totals
      [] -> nil
    end
  end

  defp all_records(table) do
    :mnesia.match_object({table, :_, :_})
    |> Enum.map(fn {^table, _key, record} -> record end)
  end

  defp normalize_record(record) when is_map(record) do
    Map.new(record)
  end

  defp interrupt_running_records(error, now) do
    @runs_table
    |> all_records()
    |> Enum.reduce(0, &interrupt_running_record(&1, error, now, &2))
  end

  defp interrupt_running_record(%{status: "running"} = record, error, now, count) do
    write_interrupted_run_record(record, error, now, count)
  end

  defp interrupt_running_record(_record, _error, _now, count), do: count

  defp write_interrupted_run_record(record, error, now, count) do
    case Map.get(record, :run_id) do
      run_id when is_binary(run_id) ->
        updated =
          Map.merge(record, %{
            status: "failure",
            ended_at: now,
            error: error,
            updated_at: now
          })

        :mnesia.write({@runs_table, run_id, updated})
        count + 1

      malformed_run_id ->
        Logger.warning("Skipping malformed running run store record during startup recovery run_id=#{inspect(malformed_run_id)}")
        count
    end
  end

  defp datetime_sort_key(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :microsecond)
  defp datetime_sort_key(_datetime), do: 0

  defp unwrap_nested_error({:error, reason}), do: {:error, reason}
  defp unwrap_nested_error(other), do: other

  @impl true
  def terminate(reason, %State{dir: dir}) do
    if reason not in [:normal, :shutdown] do
      Logger.warning("RunStore stopped unexpectedly dir=#{dir} reason=#{inspect(reason)}")
    end

    :ok
  end
end
