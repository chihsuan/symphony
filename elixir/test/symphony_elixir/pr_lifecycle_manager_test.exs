defmodule SymphonyElixir.PrLifecycleManagerTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Linear.Issue
  alias SymphonyElixir.PrLifecycleManager

  defmodule FakeTracker do
    alias SymphonyElixir.Linear.Issue

    @spec fetch_issues_by_states([String.t()]) :: {:ok, [Issue.t()]}
    def fetch_issues_by_states(_states) do
      {:ok, Application.get_env(:symphony_elixir, :pr_lifecycle_test_issues, [])}
    end

    @spec fetch_issue_states_by_ids([String.t()]) :: {:ok, [Issue.t()]}
    def fetch_issue_states_by_ids(issue_ids) do
      wanted = MapSet.new(issue_ids)

      issues =
        :symphony_elixir
        |> Application.get_env(:pr_lifecycle_test_issues, [])
        |> Enum.filter(fn %Issue{id: id} -> MapSet.member?(wanted, id) end)

      {:ok, issues}
    end
  end

  defmodule FakeGitHub do
    @spec fetch_activity(String.t(), keyword()) :: {:ok, map()}
    def fetch_activity(_pr_url, _opts) do
      {:ok, Application.fetch_env!(:symphony_elixir, :pr_lifecycle_test_activity)}
    end
  end

  defmodule FakeWorkspace do
    @spec remove(String.t(), String.t() | nil) :: {:ok, [String.t()]}
    def remove(workspace_path, worker_host) do
      recipient = Application.fetch_env!(:symphony_elixir, :pr_lifecycle_test_recipient)
      send(recipient, {:remove_workspace, workspace_path, worker_host})

      {:ok, [workspace_path]}
    end
  end

  setup do
    on_exit(fn ->
      Application.delete_env(:symphony_elixir, :pr_lifecycle_test_issues)
      Application.delete_env(:symphony_elixir, :pr_lifecycle_test_activity)
      Application.delete_env(:symphony_elixir, :pr_lifecycle_test_recipient)
    end)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      pr_lifecycle_mode: "daemon",
      pr_lifecycle_cooldown_minutes: 30,
      pr_lifecycle_stale_days: 7
    )

    Application.put_env(:symphony_elixir, :pr_lifecycle_test_recipient, self())
    :ok
  end

  test "discovers in-review PRs and persists workspace tracking metadata" do
    now = ~U[2026-05-01 09:00:00Z]
    issue = in_review_issue(updated_at: now)
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_issues, [issue])
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_activity, open_activity(now))

    assert :ok =
             RunStore.put_run(%{
               run_id: "run-1",
               issue_id: issue.id,
               issue_identifier: issue.identifier,
               status: "success",
               workspace_path: "/tmp/workspaces/RSM-1780",
               worker_host: nil,
               started_at: DateTime.add(now, -120, :second),
               ended_at: DateTime.add(now, -60, :second)
             })

    assert {:ok, %{discovered: 1, processed: 1}} =
             PrLifecycleManager.poll_once(tracker: FakeTracker, github: FakeGitHub, now: now)

    assert [
             %{
               issue_id: "issue-1780",
               issue_identifier: "RSM-1780",
               pr_url: "https://github.com/example/repo/pull/1780",
               workspace_path: "/tmp/workspaces/RSM-1780",
               status: "watching"
             }
           ] = RunStore.list_pr_lifecycles()
  end

  test "waits for cooldown before spawning a rework agent with review comments" do
    now = ~U[2026-05-01 09:00:00Z]
    latest_review_at = DateTime.add(now, -10, :minute)
    issue = in_review_issue(updated_at: now)
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_issues, [issue])

    :ok = put_lifecycle(now)

    Application.put_env(
      :symphony_elixir,
      :pr_lifecycle_test_activity,
      open_activity(latest_review_at,
        review_decision: "CHANGES_REQUESTED",
        comments: [%{kind: "inline_comment", author: "reviewer", body: "Please split this.", url: "https://github.com/example/repo/pull/1780#discussion_r1"}]
      )
    )

    assert {:ok, %{actions: [{:cooling_down, "issue-1780"}]}} =
             PrLifecycleManager.poll_once(tracker: FakeTracker, github: FakeGitHub, now: now)

    refute_receive {:agent_started, _issue, _opts}

    latest_review_at = DateTime.add(now, -31, :minute)

    Application.put_env(
      :symphony_elixir,
      :pr_lifecycle_test_activity,
      open_activity(latest_review_at,
        review_decision: "CHANGES_REQUESTED",
        comments: [%{kind: "inline_comment", author: "reviewer", body: "Please split this.", url: "https://github.com/example/repo/pull/1780#discussion_r1"}]
      )
    )

    assert {:ok, %{actions: [{:spawned, "issue-1780", :rework}]}} =
             PrLifecycleManager.poll_once(
               tracker: FakeTracker,
               github: FakeGitHub,
               now: now,
               agent_starter: agent_starter()
             )

    assert_receive {:agent_started, %Issue{id: "issue-1780"}, opts}
    assert opts[:workspace_path] == "/tmp/workspaces/RSM-1780"
    assert opts[:extra_prompt] =~ "GitHub reports requested changes"
    assert opts[:extra_prompt] =~ "Please split this."
  end

  test "spawns a merge agent when GitHub reports approval" do
    now = ~U[2026-05-01 09:00:00Z]
    issue = in_review_issue(updated_at: now)
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_issues, [issue])
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_activity, open_activity(now, review_decision: "APPROVED"))
    :ok = put_lifecycle(now)

    assert {:ok, %{actions: [{:spawned, "issue-1780", :merge}]}} =
             PrLifecycleManager.poll_once(
               tracker: FakeTracker,
               github: FakeGitHub,
               now: now,
               agent_starter: agent_starter()
             )

    assert_receive {:agent_started, %Issue{id: "issue-1780"}, opts}
    assert opts[:extra_prompt] =~ "GitHub reports approval"
    assert opts[:extra_prompt] =~ "land"
  end

  test "does not respawn rework when only PR activity changed after handled review comments" do
    now = ~U[2026-05-01 09:00:00Z]
    review_activity_at = DateTime.add(now, -90, :minute)
    latest_pr_activity_at = DateTime.add(now, -10, :minute)
    last_action_at = DateTime.add(review_activity_at, 30, :minute)

    Application.put_env(:symphony_elixir, :pr_lifecycle_test_issues, [in_review_issue(updated_at: now)])

    :ok =
      put_lifecycle(now, %{
        status: "rework_spawned",
        last_action: "rework",
        last_action_at: last_action_at,
        last_activity_at: review_activity_at,
        last_review_activity_at: review_activity_at
      })

    Application.put_env(
      :symphony_elixir,
      :pr_lifecycle_test_activity,
      open_activity(latest_pr_activity_at,
        latest_review_activity_at: review_activity_at,
        review_decision: "CHANGES_REQUESTED",
        comments: [%{kind: "review", author: "reviewer", body: "Already handled.", url: "https://github.com/example/repo/pull/1780#pullrequestreview-1"}]
      )
    )

    assert {:ok, %{actions: [{:already_handled, "issue-1780", :rework}]}} =
             PrLifecycleManager.poll_once(
               tracker: FakeTracker,
               github: FakeGitHub,
               now: now,
               agent_starter: agent_starter()
             )

    refute_receive {:agent_started, _issue, _opts}

    assert [
             %{
               status: "watching",
               last_activity_at: ^latest_pr_activity_at,
               last_review_activity_at: ^review_activity_at
             }
           ] = RunStore.list_pr_lifecycles()
  end

  test "records spawn errors without crashing the poll" do
    now = ~U[2026-05-01 09:00:00Z]
    latest_review_at = DateTime.add(now, -31, :minute)
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_issues, [in_review_issue(updated_at: now)])

    Application.put_env(
      :symphony_elixir,
      :pr_lifecycle_test_activity,
      open_activity(latest_review_at, review_decision: "APPROVED")
    )

    :ok = put_lifecycle(now)

    assert {:ok, %{actions: [{:spawn_error, "issue-1780", :merge, :boom}]}} =
             PrLifecycleManager.poll_once(
               tracker: FakeTracker,
               github: FakeGitHub,
               now: now,
               agent_starter: fn _issue, _opts -> {:error, :boom} end
             )

    assert [%{status: "spawn_error", error: ":boom", last_action: "merge"}] =
             RunStore.list_pr_lifecycles()
  end

  test "cleans up workspace and tracking when PR is closed or stale" do
    now = ~U[2026-05-01 09:00:00Z]
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_issues, [in_review_issue(updated_at: now)])
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_activity, open_activity(now, state: "MERGED"))
    :ok = put_lifecycle(now)

    assert {:ok, %{actions: [{:cleanup, "issue-1780", "closed"}]}} =
             PrLifecycleManager.poll_once(
               tracker: FakeTracker,
               github: FakeGitHub,
               workspace: FakeWorkspace,
               now: now
             )

    assert_receive {:remove_workspace, "/tmp/workspaces/RSM-1780", nil}
    assert [] = RunStore.list_pr_lifecycles()

    stale_activity_at = DateTime.add(now, -8, :day)
    Application.put_env(:symphony_elixir, :pr_lifecycle_test_activity, open_activity(stale_activity_at))
    :ok = put_lifecycle(now)

    assert {:ok, %{actions: [{:cleanup, "issue-1780", "stale"}]}} =
             PrLifecycleManager.poll_once(
               tracker: FakeTracker,
               github: FakeGitHub,
               workspace: FakeWorkspace,
               now: now
             )

    assert_receive {:remove_workspace, "/tmp/workspaces/RSM-1780", nil}
    assert [] = RunStore.list_pr_lifecycles()
  end

  defp agent_starter do
    recipient = self()

    fn issue, opts ->
      send(recipient, {:agent_started, issue, opts})
      :ok
    end
  end

  defp put_lifecycle(now, attrs \\ %{}) do
    %{
      issue_id: "issue-1780",
      issue_identifier: "RSM-1780",
      issue_url: "https://linear.app/a8c/issue/RSM-1780",
      pr_url: "https://github.com/example/repo/pull/1780",
      workspace_path: "/tmp/workspaces/RSM-1780",
      worker_host: nil,
      status: "watching",
      inserted_at: now,
      updated_at: now
    }
    |> Map.merge(attrs)
    |> RunStore.put_pr_lifecycle()
  end

  defp in_review_issue(opts) do
    updated_at = Keyword.fetch!(opts, :updated_at)

    %Issue{
      id: "issue-1780",
      identifier: "RSM-1780",
      title: "Lifecycle manager",
      description: "Poll PR state",
      state: "In Review",
      url: "https://linear.app/a8c/issue/RSM-1780",
      pr_urls: ["https://github.com/example/repo/pull/1780"],
      updated_at: updated_at
    }
  end

  defp open_activity(latest_activity_at, opts \\ []) do
    %{
      pr_url: "https://github.com/example/repo/pull/1780",
      state: Keyword.get(opts, :state, "OPEN"),
      review_decision: Keyword.get(opts, :review_decision),
      latest_activity_at: latest_activity_at,
      latest_review_activity_at: Keyword.get(opts, :latest_review_activity_at, latest_activity_at),
      comments: Keyword.get(opts, :comments, [])
    }
  end
end
