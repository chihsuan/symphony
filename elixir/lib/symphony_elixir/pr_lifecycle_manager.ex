defmodule SymphonyElixir.PrLifecycleManager do
  @moduledoc """
  Daemon-mode pull request lifecycle manager.
  """

  use GenServer
  require Logger

  alias SymphonyElixir.{AgentRunner, Config, RunStore, Tracker, Workspace}
  alias SymphonyElixir.GitHub.PullRequest
  alias SymphonyElixir.Linear.Issue

  @in_review_state "In Review"
  @changes_requested "CHANGES_REQUESTED"
  @approved "APPROVED"
  @closed_pr_states ["CLOSED", "MERGED"]

  defmodule State do
    @moduledoc false
    defstruct [:timer_ref, opts: []]
  end

  @type poll_summary :: %{
          mode: :daemon | :linear,
          discovered: non_neg_integer(),
          processed: non_neg_integer(),
          actions: [term()]
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    {:ok, schedule_poll(%State{opts: opts}, 0)}
  end

  @impl true
  def handle_info(:poll, %State{} = state) do
    case poll_once(state.opts) do
      {:ok, summary} ->
        Logger.debug("PR lifecycle poll completed: #{inspect(summary)}")

      {:error, reason} ->
        Logger.warning("PR lifecycle poll failed: #{inspect(reason)}")
    end

    {:noreply, schedule_poll(state, poll_interval_ms(state.opts))}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @doc false
  @spec poll_once(keyword()) :: {:ok, poll_summary()} | {:error, term()}
  def poll_once(opts \\ []) when is_list(opts) do
    settings = Keyword.get(opts, :settings) || Config.settings!()

    case settings.pr_lifecycle.mode do
      "daemon" ->
        do_poll_once(settings, opts)

      _mode ->
        {:ok, %{mode: :linear, discovered: 0, processed: 0, actions: []}}
    end
  rescue
    error -> {:error, {error.__struct__, Exception.message(error)}}
  end

  defp do_poll_once(settings, opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now())
    run_store = Keyword.get(opts, :run_store, RunStore)
    tracker = Keyword.get(opts, :tracker, Tracker)

    with {:ok, discovered} <- discover_lifecycles(run_store, tracker, now),
         {:ok, lifecycles} <- list_pr_lifecycles(run_store),
         {:ok, issues_by_id} <- fetch_lifecycle_issues(tracker, lifecycles) do
      actions =
        lifecycles
        |> Enum.map(&process_lifecycle(&1, Map.get(issues_by_id, Map.get(&1, :issue_id)), settings, opts, now))

      {:ok, %{mode: :daemon, discovered: discovered, processed: length(lifecycles), actions: actions}}
    end
  end

  defp discover_lifecycles(run_store, tracker, now) do
    with {:ok, issues} <- tracker.fetch_issues_by_states([@in_review_state]),
         {:ok, runs} <- list_runs(run_store),
         {:ok, existing} <- list_pr_lifecycles(run_store) do
      existing_by_issue = Map.new(existing, &{Map.get(&1, :issue_id), &1})

      discovered =
        issues
        |> Enum.filter(&match?(%Issue{}, &1))
        |> Enum.count(&persist_discovered_lifecycle?(&1, runs, existing_by_issue, run_store, now))

      {:ok, discovered}
    end
  end

  defp persist_discovered_lifecycle?(%Issue{} = issue, runs, existing_by_issue, run_store, now) do
    existing = Map.get(existing_by_issue, issue.id)

    case discover_lifecycle_record(issue, runs, existing, now) do
      nil ->
        false

      record ->
        :ok = persist_pr_lifecycle(run_store, record)
        true
    end
  end

  defp discover_lifecycle_record(%Issue{} = issue, runs, existing, now) when is_list(runs) do
    with pr_url when is_binary(pr_url) <- first_pr_url(issue),
         %{workspace_path: workspace_path} = run when is_binary(workspace_path) <-
           latest_run_for_issue(runs, issue.id) do
      base = %{
        issue_id: issue.id,
        issue_identifier: issue.identifier,
        issue_url: issue.url,
        pr_url: pr_url,
        workspace_path: workspace_path,
        worker_host: Map.get(run, :worker_host),
        status: Map.get(existing || %{}, :status, "watching"),
        inserted_at: Map.get(existing || %{}, :inserted_at, now),
        updated_at: now
      }

      Map.merge(existing || %{}, base)
    else
      _ -> nil
    end
  end

  defp discover_lifecycle_record(_issue, _runs, _existing, _now), do: nil

  defp process_lifecycle(record, issue, settings, opts, now) when is_map(record) do
    github = Keyword.get(opts, :github, PullRequest)

    case github.fetch_activity(Map.get(record, :pr_url), cwd: Map.get(record, :workspace_path)) do
      {:ok, activity} ->
        handle_activity(record, issue_for_record(issue, record), activity, settings, opts, now)

      {:error, reason} ->
        update_lifecycle(opts, record, %{
          status: "poll_error",
          error: inspect(reason),
          updated_at: now
        })

        {:poll_error, Map.get(record, :issue_id), reason}
    end
  end

  defp handle_activity(record, issue, activity, settings, opts, now) do
    {attrs, latest_activity_at} = lifecycle_activity_attrs(record, activity, now)

    case lifecycle_action(activity, latest_activity_at, settings, now) do
      :closed ->
        cleanup_lifecycle(record, opts, now, "closed")

      :stale ->
        cleanup_lifecycle(record, opts, now, "stale")

      :changes_requested ->
        maybe_spawn_rework(record, issue, activity, attrs, settings, opts, now)

      :approved ->
        maybe_spawn_merge(record, issue, activity, attrs, opts, now)

      :watching ->
        update_lifecycle(opts, record, attrs)
        {:watching, Map.get(record, :issue_id)}
    end
  end

  defp lifecycle_activity_attrs(record, activity, now) do
    latest_activity_at =
      Map.get(activity, :latest_activity_at) ||
        Map.get(record, :last_activity_at) ||
        Map.get(record, :updated_at) ||
        now

    latest_review_activity_at =
      Map.get(activity, :latest_review_activity_at) ||
        Map.get(record, :last_review_activity_at) ||
        latest_activity_at

    attrs = %{
      status: "watching",
      error: nil,
      last_activity_at: latest_activity_at,
      last_review_activity_at: latest_review_activity_at,
      last_review_decision: Map.get(activity, :review_decision),
      updated_at: now
    }

    {attrs, latest_activity_at}
  end

  defp lifecycle_action(activity, latest_activity_at, settings, now) do
    review_decision = normalize_decision(Map.get(activity, :review_decision))

    cond do
      closed_pr_state?(Map.get(activity, :state)) -> :closed
      stale?(latest_activity_at, now, settings.pr_lifecycle.stale_days) -> :stale
      review_decision == @changes_requested -> :changes_requested
      review_decision == @approved -> :approved
      true -> :watching
    end
  end

  defp maybe_spawn_rework(record, issue, activity, attrs, settings, opts, now) do
    latest_activity_at = action_activity_at(attrs)

    cond do
      handled_activity?(record, latest_activity_at) ->
        update_lifecycle(opts, record, attrs)
        {:already_handled, Map.get(record, :issue_id), :rework}

      !cooldown_elapsed?(latest_activity_at, now, settings.pr_lifecycle.cooldown_minutes) ->
        update_lifecycle(opts, record, Map.merge(attrs, %{status: "cooling_down"}))
        {:cooling_down, Map.get(record, :issue_id)}

      true ->
        prompt = rework_prompt(record, activity, settings)

        finish_spawn_action(spawn_agent(issue, record, prompt, opts), record, attrs, opts, now, "rework")
    end
  end

  defp maybe_spawn_merge(record, issue, activity, attrs, opts, now) do
    latest_activity_at = action_activity_at(attrs)

    if handled_activity?(record, latest_activity_at) do
      update_lifecycle(opts, record, attrs)
      {:already_handled, Map.get(record, :issue_id), :merge}
    else
      prompt = merge_prompt(record, activity)

      finish_spawn_action(spawn_agent(issue, record, prompt, opts), record, attrs, opts, now, "merge")
    end
  end

  defp action_activity_at(attrs) do
    Map.get(attrs, :last_review_activity_at) || Map.fetch!(attrs, :last_activity_at)
  end

  defp finish_spawn_action(:ok, record, attrs, opts, now, action) do
    update_lifecycle(
      opts,
      record,
      Map.merge(attrs, %{
        status: "#{action}_spawned",
        last_action: action,
        last_action_at: now
      })
    )

    {:spawned, Map.get(record, :issue_id), String.to_existing_atom(action)}
  end

  defp finish_spawn_action({:error, reason}, record, attrs, opts, now, action) do
    update_lifecycle(
      opts,
      record,
      Map.merge(attrs, %{
        status: "spawn_error",
        error: inspect(reason),
        last_action: action,
        updated_at: now
      })
    )

    {:spawn_error, Map.get(record, :issue_id), String.to_existing_atom(action), reason}
  end

  defp cleanup_lifecycle(record, opts, now, reason) do
    workspace = Keyword.get(opts, :workspace, Workspace)
    run_store = Keyword.get(opts, :run_store, RunStore)

    case workspace.remove(Map.get(record, :workspace_path), Map.get(record, :worker_host)) do
      {:ok, _removed_paths} ->
        :ok = run_store.delete_pr_lifecycle(Map.get(record, :issue_id))
        {:cleanup, Map.get(record, :issue_id), reason}

      {:error, cleanup_reason, output} ->
        update_lifecycle(opts, record, %{
          status: "cleanup_error",
          error: inspect({cleanup_reason, output}),
          updated_at: now
        })

        {:cleanup_error, Map.get(record, :issue_id), cleanup_reason}

      other ->
        update_lifecycle(opts, record, %{
          status: "cleanup_error",
          error: inspect(other),
          updated_at: now
        })

        {:cleanup_error, Map.get(record, :issue_id), other}
    end
  end

  defp spawn_agent(%Issue{} = issue, record, prompt, opts) do
    run_opts = [
      workspace_path: Map.get(record, :workspace_path),
      worker_host: Map.get(record, :worker_host),
      extra_prompt: prompt,
      max_turns: 1
    ]

    opts
    |> agent_starter()
    |> then(& &1.(issue, run_opts))
  end

  defp agent_starter(opts) do
    case Keyword.get(opts, :agent_starter) do
      starter when is_function(starter, 2) -> starter
      _ -> &start_agent_task(&1, &2, opts)
    end
  end

  defp start_agent_task(issue, run_opts, opts) do
    task_supervisor = Keyword.get(opts, :task_supervisor, SymphonyElixir.TaskSupervisor)
    agent_runner = Keyword.get(opts, :agent_runner, AgentRunner)

    case Task.Supervisor.start_child(task_supervisor, fn -> agent_runner.run(issue, nil, run_opts) end) do
      {:ok, _pid} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp rework_prompt(record, activity, settings) do
    """
    PR lifecycle daemon event:

    GitHub reports requested changes for #{Map.get(record, :pr_url)}. Waited at least #{settings.pr_lifecycle.cooldown_minutes} minute(s) since the latest review activity before starting this run.

    Continue in the existing workspace, address every actionable review comment below, rerun the relevant validation, push the branch, and keep the issue in review unless a true blocker remains.

    Review comments:
    #{format_comments(Map.get(activity, :comments, []))}
    """
    |> String.trim()
  end

  defp merge_prompt(record, _activity) do
    """
    PR lifecycle daemon event:

    GitHub reports approval for #{Map.get(record, :pr_url)}. Continue in the existing workspace and execute the repository landing flow for the approved PR. Follow the local `land` skill instructions; do not call `gh pr merge` directly outside that flow.
    """
    |> String.trim()
  end

  defp format_comments([]), do: "- No review comment bodies were returned by GitHub; inspect the PR before changing code."

  defp format_comments(comments) when is_list(comments) do
    comments
    |> Enum.reject(&blank?(Map.get(&1, :body)))
    |> Enum.map_join("\n\n", fn comment ->
      author = Map.get(comment, :author) || "unknown"
      kind = Map.get(comment, :kind) || "comment"
      url = Map.get(comment, :url) || "no URL"

      "- #{kind} by #{author} (#{url}):\n  " <>
        (comment
         |> Map.get(:body, "")
         |> String.split("\n")
         |> Enum.map_join("\n  ", &String.trim_trailing/1))
    end)
    |> case do
      "" -> format_comments([])
      formatted -> formatted
    end
  end

  defp issue_for_record(%Issue{} = issue, _record), do: issue

  defp issue_for_record(_issue, record) do
    %Issue{
      id: Map.get(record, :issue_id),
      identifier: Map.get(record, :issue_identifier),
      title: "PR lifecycle follow-up",
      state: @in_review_state,
      url: Map.get(record, :issue_url),
      pr_urls: [Map.get(record, :pr_url)] |> Enum.reject(&is_nil/1)
    }
  end

  defp fetch_lifecycle_issues(_tracker, []), do: {:ok, %{}}

  defp fetch_lifecycle_issues(tracker, lifecycles) do
    issue_ids =
      lifecycles
      |> Enum.map(&Map.get(&1, :issue_id))
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    case tracker.fetch_issue_states_by_ids(issue_ids) do
      {:ok, issues} ->
        {:ok, Map.new(issues, fn %Issue{id: id} = issue -> {id, issue} end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp first_pr_url(%Issue{pr_urls: [url | _rest]}) when is_binary(url), do: url
  defp first_pr_url(_issue), do: nil

  defp latest_run_for_issue(runs, issue_id) when is_list(runs) and is_binary(issue_id) do
    Enum.find(runs, fn run ->
      Map.get(run, :issue_id) == issue_id and
        Map.get(run, :status) in ["success", "stopped"] and
        is_binary(Map.get(run, :workspace_path))
    end)
  end

  defp handled_activity?(record, latest_activity_at) do
    case {Map.get(record, :last_action_at), latest_activity_at} do
      {%DateTime{} = last_action_at, %DateTime{} = latest} ->
        DateTime.compare(last_action_at, latest) in [:gt, :eq]

      _ ->
        false
    end
  end

  defp cooldown_elapsed?(%DateTime{} = latest_activity_at, %DateTime{} = now, cooldown_minutes) do
    DateTime.diff(now, latest_activity_at, :second) >= cooldown_minutes * 60
  end

  defp cooldown_elapsed?(_latest_activity_at, _now, _cooldown_minutes), do: true

  defp stale?(%DateTime{} = latest_activity_at, %DateTime{} = now, stale_days) do
    DateTime.diff(now, latest_activity_at, :day) >= stale_days
  end

  defp stale?(_latest_activity_at, _now, _stale_days), do: false

  defp closed_pr_state?(state) do
    state
    |> normalize_decision()
    |> then(&(&1 in @closed_pr_states))
  end

  defp normalize_decision(value) when is_binary(value) do
    value |> String.trim() |> String.upcase()
  end

  defp normalize_decision(_value), do: nil

  defp list_runs(run_store) do
    case run_store.list_runs(:all) do
      runs when is_list(runs) -> {:ok, runs}
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_pr_lifecycles(run_store) do
    case run_store.list_pr_lifecycles() do
      lifecycles when is_list(lifecycles) -> {:ok, lifecycles}
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_pr_lifecycle(run_store, record) do
    case run_store.put_pr_lifecycle(record) do
      :ok -> :ok
      {:error, reason} -> raise "failed to persist PR lifecycle record: #{inspect(reason)}"
    end
  end

  defp update_lifecycle(opts, record, attrs) do
    run_store = Keyword.get(opts, :run_store, RunStore)
    issue_id = Map.get(record, :issue_id)

    case run_store.update_pr_lifecycle(issue_id, attrs) do
      :ok ->
        :ok

      {:error, :pr_lifecycle_not_found} ->
        :ok = run_store.put_pr_lifecycle(Map.merge(record, attrs))

      {:error, reason} ->
        Logger.warning("Failed to update PR lifecycle record issue_id=#{issue_id}: #{inspect(reason)}")
        :ok
    end
  end

  defp schedule_poll(%State{} = state, delay_ms) when is_integer(delay_ms) and delay_ms >= 0 do
    if is_reference(state.timer_ref) do
      Process.cancel_timer(state.timer_ref)
    end

    %{state | timer_ref: Process.send_after(self(), :poll, delay_ms)}
  end

  defp poll_interval_ms(opts) do
    case Keyword.get(opts, :poll_interval_ms) do
      interval when is_integer(interval) and interval > 0 -> interval
      _ -> Config.settings!().polling.interval_ms
    end
  end

  defp blank?(value) when is_binary(value), do: String.trim(value) == ""
  defp blank?(nil), do: true
  defp blank?(_value), do: false
end
