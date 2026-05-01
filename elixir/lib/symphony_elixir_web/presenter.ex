defmodule SymphonyElixirWeb.Presenter do
  @moduledoc """
  Shared projections for the observability API and dashboard.
  """

  alias SymphonyElixir.{Config, Orchestrator, StatusDashboard, URLUtils}

  @empty_codex_totals %{
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    seconds_running: 0
  }

  @spec state_payload(GenServer.name(), timeout()) :: map()
  def state_payload(orchestrator, snapshot_timeout_ms) do
    generated_at = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    case Orchestrator.snapshot(orchestrator, snapshot_timeout_ms) do
      %{} = snapshot ->
        %{
          generated_at: generated_at,
          counts: %{
            running: length(snapshot.running),
            watching: length(Map.get(snapshot, :watching, [])),
            retrying: length(snapshot.retrying)
          },
          running: Enum.map(snapshot.running, &running_entry_payload/1),
          watching: snapshot |> Map.get(:watching, []) |> Enum.map(&watching_entry_payload/1),
          retrying: Enum.map(snapshot.retrying, &retry_entry_payload/1),
          run_history: Enum.map(Map.get(snapshot, :run_history, []), &run_history_payload/1),
          codex_totals: normalize_codex_totals(Map.get(snapshot, :codex_totals)),
          rate_limits: snapshot.rate_limits
        }

      :timeout ->
        %{generated_at: generated_at, error: %{code: "snapshot_timeout", message: "Snapshot timed out"}}

      :unavailable ->
        %{generated_at: generated_at, error: %{code: "snapshot_unavailable", message: "Snapshot unavailable"}}
    end
  end

  @spec issue_payload(String.t(), GenServer.name(), timeout()) :: {:ok, map()} | {:error, :issue_not_found}
  def issue_payload(issue_identifier, orchestrator, snapshot_timeout_ms) when is_binary(issue_identifier) do
    case Orchestrator.snapshot(orchestrator, snapshot_timeout_ms) do
      %{} = snapshot ->
        running = Enum.find(snapshot.running, &(&1.identifier == issue_identifier))
        retry = Enum.find(snapshot.retrying, &(&1.identifier == issue_identifier))
        watching = snapshot |> Map.get(:watching, []) |> Enum.find(&(&1.identifier == issue_identifier))

        if is_nil(running) and is_nil(retry) and is_nil(watching) do
          {:error, :issue_not_found}
        else
          {:ok, issue_payload_body(issue_identifier, running, retry, watching)}
        end

      _ ->
        {:error, :issue_not_found}
    end
  end

  @spec transcript_payload(String.t(), GenServer.name(), timeout()) ::
          {:ok, map()} | {:error, :issue_not_found | :snapshot_unavailable}
  def transcript_payload(issue_identifier, orchestrator, snapshot_timeout_ms)
      when is_binary(issue_identifier) do
    case Orchestrator.snapshot(orchestrator, snapshot_timeout_ms) do
      %{} = snapshot ->
        case Enum.find(snapshot.running, &(&1.identifier == issue_identifier)) do
          nil ->
            {:error, :issue_not_found}

          running ->
            {:ok,
             %{
               issue_id: running.issue_id,
               issue_identifier: running.identifier,
               state: running.state,
               session_id: running.session_id,
               started_at: iso8601(running.started_at),
               last_event_at: iso8601(running.last_codex_timestamp),
               turn_count: Map.get(running, :turn_count, 0),
               tokens: %{
                 input_tokens: running.codex_input_tokens,
                 output_tokens: running.codex_output_tokens,
                 total_tokens: running.codex_total_tokens
               },
               events: transcript_events(running)
             }}
        end

      _ ->
        {:error, :snapshot_unavailable}
    end
  end

  @spec refresh_payload(GenServer.name()) :: {:ok, map()} | {:error, :unavailable}
  def refresh_payload(orchestrator) do
    case Orchestrator.request_refresh(orchestrator) do
      :unavailable ->
        {:error, :unavailable}

      payload ->
        {:ok, Map.update!(payload, :requested_at, &DateTime.to_iso8601/1)}
    end
  end

  defp issue_payload_body(issue_identifier, running, retry, watching) do
    payload = %{
      issue_identifier: issue_identifier,
      issue_id: issue_id_from_entries(running, retry, watching),
      status: issue_status(running, retry, watching),
      workspace: workspace_payload(issue_identifier, running, retry),
      attempts: %{
        restart_count: restart_count(retry),
        current_retry_attempt: retry_attempt(retry)
      },
      running: running && running_issue_payload(running),
      retry: retry && retry_issue_payload(retry),
      logs: %{
        codex_session_logs: []
      },
      recent_events: (running && recent_events_payload(running)) || [],
      last_error: retry && retry.error,
      tracked: %{}
    }

    if watching do
      Map.put(payload, :watching, watching_issue_payload(watching))
    else
      payload
    end
  end

  defp issue_id_from_entries(running, retry, watching),
    do: (running && running.issue_id) || (retry && retry.issue_id) || (watching && watching.issue_id)

  defp restart_count(retry), do: max(retry_attempt(retry) - 1, 0)
  defp retry_attempt(nil), do: 0
  defp retry_attempt(retry), do: retry.attempt || 0

  defp issue_status(running, retry, _watching) do
    cond do
      running -> "running"
      retry -> "retrying"
      true -> "watching"
    end
  end

  defp running_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      state: entry.state,
      url: URLUtils.present_url(Map.get(entry, :url)),
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path),
      session_id: entry.session_id,
      transcript_path: Map.get(entry, :transcript_path),
      turn_count: Map.get(entry, :turn_count, 0),
      last_event: entry.last_codex_event,
      last_message: summarize_message(entry.last_codex_message),
      started_at: iso8601(entry.started_at),
      last_event_at: iso8601(entry.last_codex_timestamp),
      tokens: %{
        input_tokens: entry.codex_input_tokens,
        output_tokens: entry.codex_output_tokens,
        total_tokens: entry.codex_total_tokens
      }
    }
  end

  defp watching_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      state: entry.state,
      url: URLUtils.present_url(Map.get(entry, :url)),
      pull_request_url: URLUtils.pull_request_url(entry),
      last_ran_at: iso8601(entry.last_ran_at),
      seconds_since_last_run: entry.seconds_since_last_run
    }
  end

  defp retry_entry_payload(entry) do
    %{
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      attempt: entry.attempt,
      due_at: due_at_iso8601(entry.due_in_ms),
      error: entry.error,
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path)
    }
  end

  defp running_issue_payload(running) do
    %{
      worker_host: Map.get(running, :worker_host),
      workspace_path: Map.get(running, :workspace_path),
      session_id: running.session_id,
      transcript_path: Map.get(running, :transcript_path),
      turn_count: Map.get(running, :turn_count, 0),
      state: running.state,
      started_at: iso8601(running.started_at),
      last_event: running.last_codex_event,
      last_message: summarize_message(running.last_codex_message),
      last_event_at: iso8601(running.last_codex_timestamp),
      tokens: %{
        input_tokens: running.codex_input_tokens,
        output_tokens: running.codex_output_tokens,
        total_tokens: running.codex_total_tokens
      }
    }
  end

  defp retry_issue_payload(retry) do
    %{
      attempt: retry.attempt,
      due_at: due_at_iso8601(retry.due_in_ms),
      error: retry.error,
      worker_host: Map.get(retry, :worker_host),
      workspace_path: Map.get(retry, :workspace_path)
    }
  end

  defp watching_issue_payload(watching) do
    %{
      state: watching.state,
      url: URLUtils.present_url(watching.url),
      pull_request_url: URLUtils.pull_request_url(watching),
      last_ran_at: iso8601(watching.last_ran_at),
      seconds_since_last_run: watching.seconds_since_last_run
    }
  end

  defp run_history_payload(entry) do
    %{
      run_id: entry.run_id,
      issue_id: entry.issue_id,
      issue_identifier: entry.issue_identifier,
      title: Map.get(entry, :title),
      state: Map.get(entry, :state),
      status: entry.status,
      attempt: entry.attempt,
      started_at: iso8601(entry.started_at),
      ended_at: iso8601(Map.get(entry, :ended_at)),
      error: Map.get(entry, :error),
      worker_host: Map.get(entry, :worker_host),
      workspace_path: Map.get(entry, :workspace_path),
      session_id: Map.get(entry, :session_id),
      transcript_path: Map.get(entry, :transcript_path),
      turn_count: Map.get(entry, :turn_count, 0),
      runtime_seconds: Map.get(entry, :runtime_seconds, 0),
      tokens: Map.get(entry, :tokens, %{})
    }
  end

  defp normalize_codex_totals(totals) when is_map(totals) do
    Map.merge(@empty_codex_totals, totals)
  end

  defp normalize_codex_totals(_totals), do: @empty_codex_totals

  defp workspace_payload(issue_identifier, running, retry) do
    if running || retry do
      %{
        path: workspace_path(issue_identifier, running, retry),
        host: workspace_host(running, retry)
      }
    end
  end

  defp workspace_path(issue_identifier, running, retry) do
    (running && Map.get(running, :workspace_path)) ||
      (retry && Map.get(retry, :workspace_path)) ||
      Path.join(Config.settings!().workspace.root, issue_identifier)
  end

  defp workspace_host(running, retry) do
    (running && Map.get(running, :worker_host)) || (retry && Map.get(retry, :worker_host))
  end

  defp recent_events_payload(running) do
    [
      %{
        at: iso8601(running.last_codex_timestamp),
        event: running.last_codex_event,
        message: summarize_message(running.last_codex_message)
      }
    ]
    |> Enum.reject(&is_nil(&1.at))
  end

  defp transcript_events(%{transcript_buffer: events}) when is_list(events), do: events
  defp transcript_events(_running), do: []

  defp summarize_message(nil), do: nil
  defp summarize_message(message), do: StatusDashboard.humanize_codex_message(message)

  defp due_at_iso8601(due_in_ms) when is_integer(due_in_ms) do
    DateTime.utc_now()
    |> DateTime.add(div(due_in_ms, 1_000), :second)
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp due_at_iso8601(_due_in_ms), do: nil

  defp iso8601(%DateTime{} = datetime) do
    datetime
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp iso8601(_datetime), do: nil
end
